use crate::Cli;
use crate::flow::app_service::ApplicationService;
use crate::flow::restore::restore_from_backup;
use crate::flow::transactions::FileManagerTransactionService;
use millegrilles_common_rust::certificats::build_store_path_v2;
use millegrilles_common_rust::chiffrage_cle::CleChiffrageHandlerImpl;
use millegrilles_common_rust::configuration::{ConfigDb, ConfigMessages, charger_configuration, charger_configuration_mongo};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::initialiser_v3;
use millegrilles_common_rust::openssl::pkey::{PKey, Private};
use millegrilles_common_rust::tokio::task::JoinSet;
use millegrilles_common_rust::tokio_util::sync::CancellationToken;
use millegrilles_common_rust::tracing::{debug, info};
use millegrilles_common_rust::v3::facades::message_inbound::MessageInboundValidator;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::impls::backup_service::DomainBackupServiceImpl;
use millegrilles_common_rust::v3::impls::config_service::ConfigServiceDbImpl;
use millegrilles_common_rust::v3::impls::filehost_service::FilehostServiceImpl;
use millegrilles_common_rust::v3::impls::format_service::FormatServiceImpl;
use millegrilles_common_rust::v3::impls::messaging_service::MessagingServiceImpl;
use millegrilles_common_rust::v3::impls::security_service::SecurityServiceImpl;
use millegrilles_common_rust::v3::{ChiffrageService, ConfigService};
use std::sync::Arc;
use millegrilles_common_rust::chrono::Duration;
use millegrilles_common_rust::v3::facades::document_encryption_service::DocumentEncryptionServiceImpl;
use crate::constants::DOMAIN_NAME;
use crate::external::mongo::*;
use crate::flow::file_events::DelayedEventEmitterService;

pub struct AppContext {
    pub join_set: JoinSet<()>,
    pub chiffrage: Arc<dyn ChiffrageService>,
    pub outbound: Arc<MessageOutboundFacade>,
    pub shutdown_token: CancellationToken,
}

impl AppContext {
    pub async fn new(cli: &Cli, master_key: Option<PKey<Private>>) -> Result<AppContext, CommonError> {
        // Shutdown/cancel semantics
        let shutdown_token = CancellationToken::new();
        let mut join_set = JoinSet::new();

        // Basic services
        let config = Arc::new(init_config().await?);
        let security = Arc::new(init_security(config.as_ref()).await?);
        let messaging = Arc::new(MessagingServiceImpl::new(config.clone(), security.clone()));
        let format = Arc::new(FormatServiceImpl::new(config.clone()));

        let mongo = Arc::new(
            initialiser_v3(config.as_ref(), config.get_configuraiton_mongo()).await?
        );

        // Facades
        let outbound = Arc::new(
            MessageOutboundFacade::new(config.clone(), messaging.clone(), format.clone(), security.clone()));
        let inbound = Arc::new(
            MessageInboundValidator::new(config.clone(), messaging.clone(), security.clone(), shutdown_token.clone())
        );

        let transaction = Arc::new(FileManagerTransactionService::new(
            config.clone(),
            format.clone(),
            mongo.clone(),
            cli.restore,
        ));

        let filehost = Arc::new(FilehostServiceImpl::new(config.clone(), format.clone(), outbound.clone()));

        let document_encryption = Arc::new(DocumentEncryptionServiceImpl::new(security.clone(), outbound.clone(), DOMAIN_NAME));

        // List data tables (excluding redolog and tracking). They get truncated on restore (when not resuming).
        let data_tables = vec![
            // TODO COLLECTION_NAME_ACCOUNTS.to_string(),
        ];
        let backup = Arc::new(DomainBackupServiceImpl::new(
            config.clone(),
            security.clone(),
            outbound.clone(),
            security.clone(),
            mongo.clone(),
            transaction.transaction.clone(),
            filehost.clone(),
            data_tables
        ));

        let event_emitter_service = Arc::new(DelayedEventEmitterService::new(
            outbound.clone(),
            Duration::milliseconds(800),
            30
        ));

        let app_service = Arc::new(ApplicationService::new(
            security.clone(),
            config.clone(),
            security.clone(),
            messaging.clone(),
            outbound.clone(),
            transaction.clone(),
            mongo.clone(),
            backup.clone(),
            document_encryption.clone(),
            event_emitter_service.clone(),
        ));

        info!("Configure middleware resources : queues, index, tables, ...");
        app_service.configure(messaging.as_ref()).await?;

        info!("Connect services, start maintenance threads");
        start_threads(
            &mut join_set,
            security.clone(),
            messaging.as_ref(),
            inbound.clone(),
            filehost.clone(),
            app_service.clone(),
            shutdown_token.clone(),
            cli.restore,
            cli.noresume,
            master_key,
        ).await?;

        Ok(AppContext {
            join_set,
            chiffrage: security.clone(),
            outbound,
            shutdown_token,
        })
    }
}


async fn init_config() -> Result<ConfigServiceDbImpl, CommonError> {
    let config = charger_configuration()?;
    let mongo = charger_configuration_mongo(config.get_configuration_pki())?;
    Ok(ConfigServiceDbImpl::new(Arc::new(config), Arc::new(mongo)))
}


async fn init_security(config: &dyn ConfigService) -> Result<SecurityServiceImpl, CommonError> {
    let validator = build_store_path_v2(&config.get_configuration_pki().ca_certfile).map_err(|e| e.to_string())?;
    let private_key = config.get_configuration_pki().get_enveloppe_privee();

    let security_impl = SecurityServiceImpl::new(
        private_key,
        Arc::new(validator),
        Arc::new(CleChiffrageHandlerImpl::new()),
    );

    Ok(security_impl)
}

async fn start_threads(
    join_set: &mut JoinSet<()>,
    security: Arc<SecurityServiceImpl>,
    messaging: &MessagingServiceImpl,
    inbound: Arc<MessageInboundValidator>,
    filehost: Arc<FilehostServiceImpl>,
    app_service: Arc<ApplicationService>,
    shutdown_token: CancellationToken,
    is_restoring: bool,
    no_resume: bool,
    master_key: Option<PKey<Private>>,
) -> Result<(), CommonError> {

    // Connect to RabbitMQ (throws error on failure).
    // This also spawns all other required threads.
    messaging.start(join_set, shutdown_token.clone()).await?;
    debug!("Started messaging service, connection OK");

    // Spawn other service maintenance threads
    let shutdown_token_clone = shutdown_token.clone();
    join_set.spawn(async move { security.run(shutdown_token_clone).await });
    let shutdown_token_clone = shutdown_token.clone();
    join_set.spawn(async move { filehost.run(shutdown_token_clone).await });

    if ! is_restoring {
        // Spawn consumer threads
        app_service.start(join_set, inbound.clone(), shutdown_token.clone())?;
    } else {
        let master_key = match master_key {
            Some(key) => key,
            None => panic!("Master key not provided for restoring, aborting")
        };
        info!("Not starting consumer threads - restoring from backup");
        let shutdown_token_clone = shutdown_token.clone();
        join_set.spawn(async move {
            restore_from_backup(app_service, no_resume, &master_key, shutdown_token_clone).await
        });
    }

    Ok(())
}

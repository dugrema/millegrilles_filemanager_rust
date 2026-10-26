use crate::constants::*;
use crate::external::mongo::*;
use crate::external::mq::*;
use crate::flow::backup::process_backup;
use crate::flow::commands::process_command;
use crate::flow::events::process_events;
use crate::flow::maintenance::process_ticker_job;
use crate::flow::requests::process_request;
use crate::flow::transaction_commands::process_transaction;
use crate::flow::transactions::FileManagerTransactionService;
use millegrilles_common_rust::chrono::Utc;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::MongoDaoImpl;
use millegrilles_common_rust::openssl::pkey::{PKey, Private};
use millegrilles_common_rust::tokio;
use millegrilles_common_rust::tokio::task::JoinSet;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error, info};
use millegrilles_common_rust::v3::facades::document_encryption_service::DocumentEncryptionServiceImpl;
use millegrilles_common_rust::v3::facades::message_inbound::MessageInboundValidator;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::impls::backup_restorer::RestorationState;
use millegrilles_common_rust::v3::impls::messaging_service::MessagingServiceImpl;
use millegrilles_common_rust::v3::{BackupService, ChiffrageService, ConfigService, FormatService, MessagingService, PkiService};
use std::sync::Arc;
use millegrilles_common_rust::tokio_util::sync::CancellationToken;
use crate::flow::file_events::DelayedEventEmitterService;

/// Handles queue consumer threads, calls individual routing methods
pub struct ApplicationService {
    pki: Arc<dyn PkiService>,
    config: Arc<dyn ConfigService>,
    format: Arc<dyn FormatService>,
    chiffrage: Arc<dyn ChiffrageService>,
    messaging: Arc<dyn MessagingService>,
    outbound: Arc<MessageOutboundFacade>,
    transaction: Arc<FileManagerTransactionService>,
    mongo: Arc<MongoDaoImpl>,
    backup: Arc<dyn BackupService>,
    document_encryption: Arc<DocumentEncryptionServiceImpl>,
    event_emitter_service: Arc<DelayedEventEmitterService>,
}

impl ApplicationService {
    pub fn new(
        pki: Arc<dyn PkiService>,
        config: Arc<dyn ConfigService>,
        format: Arc<dyn FormatService>,
        chiffrage: Arc<dyn ChiffrageService>,
        messaging: Arc<dyn MessagingService>,
        outbound: Arc<MessageOutboundFacade>,
        transaction: Arc<FileManagerTransactionService>,
        mongo: Arc<MongoDaoImpl>,
        backup: Arc<dyn BackupService>,
        document_encryption: Arc<DocumentEncryptionServiceImpl>,
        event_emitter_service: Arc<DelayedEventEmitterService>,
    ) -> Self {
        Self {
            pki,
            config,
            format,
            chiffrage,
            messaging,
            outbound,
            transaction,
            mongo,
            backup,
            document_encryption,
            event_emitter_service,
        }
    }

    pub async fn configure(&self, mq: &MessagingServiceImpl) -> Result<(), CommonError> {
        init_queues(mq)?;
        create_index_mongodb(self.mongo.as_ref()).await?;
        Ok(())
    }

    /// Call to spawn the consumer threads
    pub fn start(
        self: Arc<Self>,
        join_set: &mut JoinSet<()>,
        incoming: Arc<MessageInboundValidator>,
        shutdown_token: CancellationToken,
    ) -> Result<(), CommonError> {

        let self_clone = self.clone();
        let incoming_clone = incoming.clone();
        join_set.spawn(async move {self_clone.process_ticker_thread(incoming_clone).await});

        let self_clone = self.clone();
        let incoming_clone = incoming.clone();
        join_set.spawn(async move {self_clone.process_events_thread(incoming_clone).await});

        let self_clone = self.clone();
        let incoming_clone = incoming.clone();
        join_set.spawn(async move {self_clone.process_requests_thread(incoming_clone).await});

        let self_clone = self.clone();
        let incoming_clone = incoming.clone();
        join_set.spawn(async move {self_clone.process_commands_thread(incoming_clone).await});

        let self_clone = self.clone();
        let incoming_clone = incoming.clone();
        join_set.spawn(async move {self_clone.process_transaction_thread(incoming_clone).await});

        let self_clone = self.clone();
        let incoming_clone = incoming.clone();
        join_set.spawn(async move {self_clone.process_backup_thread(incoming_clone).await});

        let event_emitter_service = self.event_emitter_service.clone();
        join_set.spawn(async move {event_emitter_service.run(shutdown_token).await});

        Ok(())
    }
    
    async fn process_ticker_thread(&self, incoming: Arc<MessageInboundValidator>) {
        let streamer = incoming.consume_named_queue(
            format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_TICKER).as_str(),
        ).expect("Consumer streaming init failed");
        tokio::pin!(streamer);
        while let Some(result) = streamer.next().await {
            match result {
                Ok(message) => {
                    if let Err(e) = process_ticker_job(
                        self.mongo.as_ref(),
                        self.outbound.as_ref(),
                        self.backup.as_ref(),
                        message
                    ).await {
                        error!("Ticker job failed: {}", e);
                    }
                }
                Err(e) => {
                    error!("Error processing ticker message: {}", e);
                }
            }
        }
        debug!("process_ticker_thread Closed");
    }

    async fn process_events_thread(&self, incoming: Arc<MessageInboundValidator>) {
        let streamer = incoming
            .consume_named_queue(QUEUE_EVENTS)
            .expect("Consumer streaming init failed");
        tokio::pin!(streamer);
        while let Some(result) = streamer.next().await {
            match result {
                Ok(message) => {
                    if let Err(e) = process_events(
                        self.chiffrage.as_ref(),
                        self.mongo.as_ref(),
                        self.outbound.as_ref(),
                        self.event_emitter_service.as_ref(),
                        message,
                    ).await {
                        error!("Ticker job failed: {}", e);
                    }
                }
                Err(e) => {
                    error!("Error processing ticker message: {}", e);
                }
            }
        }
        debug!("process_ticker_thread Closed");
    }

    // Requests
    async fn process_requests_thread(&self, incoming: Arc<MessageInboundValidator>) {
        let streamer = incoming.consume_named_queue(
            format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_REQUESTS).as_str(),
        ).expect("Consumer streaming init failed");
        tokio::pin!(streamer);
        while let Some(result) = streamer.next().await {
            match result {
                Ok(message) => {
                    let routing = message.message.routage.clone();
                    if let Err(e) = process_request(
                        self.config.as_ref(),
                        self.mongo.as_ref(),
                        self.outbound.as_ref(),
                        message
                    ).await {
                        error!("Request {:?} failed: {}", routing, e);
                    }
                }
                Err(e) => {
                    error!("Error processing request message: {}", e);
                }
            }
        }
        debug!("process_requests_thread Closed");
    }

    async fn process_commands_thread(&self, incoming: Arc<MessageInboundValidator>) {
        let streamer = incoming.consume_named_queue(
            format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_COMMANDS).as_str(),
        ).expect("Consumer streaming init failed");
        tokio::pin!(streamer);
        while let Some(result) = streamer.next().await {
            match result {
                Ok(message) => {
                    let routing = message.message.routage.clone();
                    if let Err(e) = process_command(
                        self.format.as_ref(),
                        self.messaging.as_ref(),
                        self.mongo.clone(),
                        self.outbound.clone(),
                        message
                    ).await {
                        error!("Request {:?} failed: {}", routing, e);
                    }
                }
                Err(e) => {
                    error!("Error processing issue message: {}", e);
                }
            }
        }
        debug!("process_certificate_signing_thread Closed");
    }

    // Transactions
    async fn process_transaction_thread(&self, incoming: Arc<MessageInboundValidator>) {
        let streamer = incoming.consume_named_queue(
            format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_TRANSACTIONS).as_str(),
        ).expect("Consumer streaming init failed");
        tokio::pin!(streamer);
        while let Some(result) = streamer.next().await {
            match result {
                Ok(message) => {
                    if let Err(e) = process_transaction(
                        self.mongo.as_ref(),
                        self.messaging.as_ref(),
                        self.outbound.as_ref(),
                        self.event_emitter_service.as_ref(),
                        self.document_encryption.as_ref(),
                        self.transaction.as_ref(),
                        message
                    ).await {
                        error!("Transaction job failed: {}", e);
                    }
                }
                Err(e) => {
                    error!("Error processing transaction message: {}", e);
                }
            }
        }
        debug!("process_transaction_thread Closed");
    }

    async fn process_backup_thread(&self, incoming: Arc<MessageInboundValidator>) {
        let streamer = incoming.consume_named_queue(
            format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_BACKUP).as_str(),
        ).expect("Consumer streaming init failed");
        tokio::pin!(streamer);
        while let Some(result) = streamer.next().await {
            match result {
                Ok(message) => {
                    if let Err(e) = process_backup(
                        self.outbound.as_ref(),
                        self.backup.as_ref(),
                        message
                    ).await {
                        error!("Reading job failed: {}", e);
                    }
                }
                Err(e) => {
                    error!("Error processing reading message: {}", e);
                }
            }
        }
        debug!("process_readings_thread Closed");
    }

    pub async fn restore(
        &self,
        master_key: Option<&PKey<Private>>,
        resume: bool,
        version: Option<String>,
    ) -> Result<RestorationState, CommonError> {
        // Wait for reply q (certificate queries)
        self.outbound.wait_ready(Some(20_000)).await?;

        let start_time = Utc::now();
        let result = self.backup.restore_domain(
            DOMAIN_NAME,    // use old domain name for backup file compatibility
            COLLECTION_NAME_REDOLOG,
            COLLECTION_NAME_TRACKING,
            resume,
            true,   // This is the domain that handles certificates, can't check while resuming
            version,
            master_key,
        ).await?;
        let duration = Utc::now() - start_time;
        info!("restore_domain duration: {} ms", duration.num_milliseconds());

        Ok(result)
    }

}

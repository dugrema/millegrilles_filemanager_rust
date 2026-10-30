use crate::constants::*;
use crate::external::mongo::*;
use crate::flow::access::maintenance_access;
use crate::flow::file_events::DelayedEventEmitterService;
use crate::flow::file_jobs::maintain_file_jobs;
use crate::flow::file_topology::maintain_visits_claims;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::{Datelike, Duration, Timelike, Utc, Weekday};
use millegrilles_common_rust::common_messages::BackupEvent;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::messages_generiques::MessageCedule;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::{BackupService, ConfigService, PresenceService, TransactionService};

pub async fn process_ticker_job<M>(
    config: &dyn ConfigService,
    transaction: &dyn TransactionService,
    mongo: &M,
    outbound: &MessageOutboundFacade,
    backup: &dyn BackupService,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    trigger: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    // Ensure this is an authorized module
    if let Err(e) = validate_ticker(&trigger).await {
        error!("Invalid ticker message, rejecting: {}", e);
        return Ok(());
    }

    let trigger_value: MessageCedule = trigger.message.deserialize()?;
    let date = trigger_value.get_date();
    let hour = date.hour();
    let minute = date.minute();
    let day = date.weekday();

    // Dev flag makes all jobs run every minute
    let is_dev = config.get_configuration_instance().dev;

    debug!("process_ticker_job for h:{} m:{}, is_dev:{}", hour, minute, is_dev);

    // Emit domain presence
    if let Err(e) = outbound.emit_domain_presence(DOMAIN_NAME, None).await {
        warn!("Error emitting domain presence: {}", e);
    }

    let trigger_date = if is_dev {
        debug!("process_ticker_job dev mode, running all jobs");
        None
    } else {
        Some(&date)
    };

    // Database maintenance

    if let Err(e) = maintain_visits_claims(transaction, mongo, outbound, delayed_event_emitter_service, trigger_date).await {
        error!("Maintenance error when processing maintenance_visits_claims: {:?}", e);
    }

    if let Err(e) = maintain_file_jobs(transaction, mongo, outbound, trigger_date).await {
        error!("Maintenance error when processing maintain_file_jobs: {:?}", e);
    }

    if let Err(e) = maintenance_access(mongo, trigger_date).await {
        error!("Maintenance error when processing maintain_file_jobs: {:?}", e);
    }

    // Backup handling
    if minute % 30 == 4 {
        // {
        // Run complete backup once a week on Sunday at 7:04 UTC.
        // This concatenates all incremental files and rotates backup files. May produce final file.
        let complete = minute == 4 && hour == 7 && day == Weekday::Sun;
        // let complete = true;

        match backup.backup_domain(
            DOMAIN_NAME,
            COLLECTION_NAME_REDOLOG,
            ! complete,  // Invert, the bool is for incremental backups (true == incremental)
        ).await {
            Ok(result) => {
                info!("Backup task completed");
                match backup.transfer_backup_files_to_filehost(DOMAIN_NAME).await {
                    Ok(()) => {
                        info!("Backup files uploaded to filehost");
                        // Emit the backup done event. This tells the filecontroler to sync backup files
                        // across all filehosts.
                        let version = match result { Some(result) => result.version, None => None };
                        outbound.emit_backup_event(BackupEvent::new_done(DOMAIN_NAME, version)).await.ok();
                    },
                    Err(e) => error!("Error uploading backup files to filehost: {}", e)
                }
            },
            Err(e) => {
                error!("Error backing up domain: {}", e);
            }
        }
    }

    // Additional file upload task in case backups keep failing.
    if minute == 13 && hour % 8 == 1 {
        // {
        if let Err(e) = backup.transfer_backup_files_to_filehost(DOMAIN_NAME).await {
            error!("Error uploading backup files to filehost: {}", e);
        }
    }

    Ok(())
}

pub const ROLE_TICKER: &str = "ceduleur";

pub async fn validate_ticker(trigger: &MessageValidated) -> Result<(), CommonError> {
    if let Ok(true) = trigger.certificate.verifier_roles_string(vec![ROLE_TICKER.to_string()]) {
        // Ok
    } else {
        return Err(CommonError::Str("Ticker message without ticker (ceduleur) role, ignoring"));
    }
    if trigger.message.estampille < Utc::now() - Duration::seconds(45) {
        debug!("Expired Ticker message, ignoring");
        return Err(CommonError::Str("Ticker message without ticker (ceduleur) role, ignoring"));
    }
    Ok(())
}


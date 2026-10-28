use std::collections::HashMap;
use crate::constants::*;
use crate::external::mongo::*;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::{Datelike, Duration, Timelike, Utc, Weekday};
use millegrilles_common_rust::common_messages::BackupEvent;
use millegrilles_common_rust::constantes::*;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::messages_generiques::MessageCedule;
use millegrilles_common_rust::mongo_dao::MongoDao;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::{BackupService, PresenceService};
use serde::{Deserialize, Serialize};

pub async fn process_ticker_job(
    mongo: &dyn MongoDao,
    outbound: &MessageOutboundFacade,
    backup: &dyn BackupService,
    trigger: MessageValidated
) -> Result<(), CommonError> {
    // Ensure this is an authorized module
    if let Err(e) = validate_ticker(&trigger).await {
        error!("Invalid ticker message, rejecting: {}", e);
        return Ok(());
    }

    let trigger_value: MessageCedule = trigger.message.deserialize()?;

    let hour = trigger_value.get_date().hour();
    let minute = trigger_value.get_date().minute();
    let day = trigger_value.get_date().weekday();

    debug!("ticker_job_ca for h:{} m:{}",hour,minute);

    // Emit domain presence
    if let Err(e) = outbound.emit_domain_presence(DOMAIN_NAME, None).await {
        warn!("Error emitting domain presence: {}", e);
    }

    // Database maintenance

    warn!("TODO - database maintenance");
    // // Process the log of filehost visits received from batch transfers
    //     if minutes % 6 == 5
    //     // if minutes % 5 == 3
    //     {
    //         if let Err(e) = process_visits(middleware).await {
    //             error!("process_visits Error processing filehost visits log: {:?}", e);
    //         }
    //     }
    //
    //     // Claim all files to avoid having them being deleted from filehosts.
    //     if hours % 8 == 1 && minutes == 14
    //     // if minutes % 5 == 0
    //     {
    //         if let Err(e) = claim_all_files(middleware).await {
    //             error!("verifier_visites Erreur entretien visites fichiers: {:?}", e);
    //         }
    //     }
    //
    //     // Check if new files have been transferred to filehosts - this complements the newFuuid event.
    //     if minutes % 6 == 0 {
    //         info!("reclamer_fichiers for new files STARTING");
    //         if let Err(e) = reclamer_fichiers(middleware, gestionnaire, true).await {
    //             error!("reclamer_fichiers Error: {:?}", e);
    //         }
    //         info!("reclamer_fichiers for new files DONE");
    //     }
    //
    //     if hours % 3 == 2 && minutes == 27
    //     {
    //         // Remove media/indexation jobs that will never complete
    //         info!("maintenance_impossible_jobs STARTING");
    //         if let Err(e) = maintenance_impossible_jobs(middleware, gestionnaire).await {
    //             error!("maintenance_impossible_jobs Error: {:?}", e);
    //         }
    //         info!("maintenance_impossible_jobs DONE");
    //     }
    //
    //     if hours % 6 == 1 && minutes == 6
    //     // if minutes % 5 == 1
    //     {
    //         if let Err(e) = maintain_deleted_files(middleware, gestionnaire).await {
    //             error!("maintain_deleted_files Error: {:?}", e);
    //         }
    //     }
    //
    //     if minutes % 30 == 13
    //     {
    //         info!("create_missing_jobs STARTING");
    //         if let Err(e) = create_missing_jobs(middleware).await {  // Creer jobs media/indexation manquantes (recovery)
    //             info!("create_missing_jobs Error: {:?}", e);
    //         }
    //         info!("create_missing_jobs DONE");
    //     }
    //
    //     if minutes % 4 == 2
    //     {
    //         // Restart expired media jobs
    //         info!("entretien_jobs_expirees STARTING");
    //         let fetch_filehosts = minutes == 2;  // Once an hour
    //         if let Err(e) = entretien_jobs_expirees(middleware, fetch_filehosts).await {
    //             error!("entretien_jobs_expirees Error: {:?}", e);
    //         }
    //         info!("entretien_jobs_expirees DONE");
    //     }
    //
    //     // Recalculer les quotas a toutes les 3 heures
    //     if hours % 3 == 1 && minutes == 14
    //     {
    //         info!("calculer_quotas STARTING");
    //         if let Err(e) = calculer_quotas(middleware).await {
    //             error!("calculer_quotas Error: {:?}", e);
    //         };
    //         info!("calculer_quotas DONE");
    //     }
    //
    //     if hours % 3 == 2 && minutes == 9 {
    //         if let Err(e) = run_cleanup_leases(middleware).await {
    //             error!("run_cleanup_leases Error: {:?}", e);
    //         }
    //     }

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

#[derive(Serialize)]
struct RequeteFuuidsVisites<'a> {
    fuuids: &'a Vec<&'a str>,
    batch_no: Option<usize>,
    done: Option<bool>,
}

#[derive(Deserialize)]
pub struct RowFuuidVisit {
    pub fuuid: String,
    pub visits: HashMap<String, i64>,
}

#[derive(Deserialize)]
pub struct RequeteGetVisitesFuuidsResponse {
    pub ok: bool,
    pub err: Option<String>,
    pub visits: Option<Vec<RowFuuidVisit>>,
    pub unknown: Option<Vec<String>>
}

pub async fn verifier_visites_topologies(
    outbound: &MessageOutboundFacade,
    fuuids: &Vec<&str>
) -> Result<RequeteGetVisitesFuuidsResponse, CommonError> {
    let request = RequeteFuuidsVisites { fuuids, batch_no: None, done: None };

    let routing = RoutageMessageAction::builder(
        DOMAINE_TOPOLOGIE,
        COMMAND_CLAIM_FILEHOST_VISITS,
        vec![Securite::L3Protege]
    ).build();
    match outbound.send_command(routing, request).await? {
        Some(response) => {
            if let (true, err) = response.is_err()? {
                Err(CommonError::String(format!("Error on command claimAndFilehostVisits: {:?}", err)))
            } else {
                Ok(response.message.deserialize::<RequeteGetVisitesFuuidsResponse>()?)
            }
        },
        None => Err(CommonError::Str("No response received for claimAndFilehostVisits command"))
    }
}

use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::common_messages::BackupEvent;
use millegrilles_common_rust::constantes::*;
use millegrilles_common_rust::v3::{BackupService, PresenceService};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::v3::models::ErrorMessage;
use crate::constants::*;
use crate::external::mongo::*;

pub async fn process_backup(
    outbound: &MessageOutboundFacade,
    backup: &dyn BackupService,
    wrapper: MessageValidated
) -> Result<(), CommonError> {
    let action = match wrapper.get_routing_action() {
        Some(action) => action,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err("No action provided in command")).await
    };

    match action {
        COMMANDE_DECLENCHER_BACKUP => trigger_complete_backup(outbound, backup, wrapper).await,
        COMMANDE_REGENERER => {
            let response = ErrorMessage {
                ok: false,
                code: Some(1),
                err: Some("Unsupported command through web interface. Use the CLI (provided script).".to_string())
            };
            outbound.respond(wrapper.delivery_info, response).await
        }
        _ => {
            warn!("process_backup_messages (CA) Unsupported command type: {}", action);
            let response = ErrorMessage { ok: false, code: Some(404), err: Some("Unsupported command".to_string()) };
            outbound.respond(wrapper.delivery_info, response).await.ok();
            Err(CommonError::Str("Bad message, unsupported action type"))
        }
    }
}

async fn trigger_complete_backup(
    outbound: &MessageOutboundFacade,
    backup: &dyn BackupService,
    wrapper: MessageValidated
) -> Result<(), CommonError> {
    // Verify authorization
    let admin = wrapper.certificate.verifier_delegation_globale(DELEGATION_GLOBALE_PROPRIETAIRE)?;
    if ! admin {
        let response = ErrorMessage { ok: false, code: Some(401), err: Some("Must be admin to trigger".to_string()) };
        outbound.respond(wrapper.delivery_info, response).await.ok();
        return Err(CommonError::Str("Access denied, must be admin"))
    } else {
        let admin_username = wrapper.certificate.get_common_name().unwrap_or("NA".to_string());
        let admin_user_id = wrapper.certificate.get_user_id()?.unwrap_or("NA".to_string());
        info!("Backup triggered by command from {} (user_id {})", admin_username, admin_user_id);
    }

    match backup.backup_domain(DOMAIN_NAME, COLLECTION_NAME_REDOLOG, false).await {
        Ok(result) => {
            let version = match result {
                Some(result) => {
                    debug!("Backup done, version: {:?}", result.version);
                    result.version
                }
                None => {
                    debug!("Backup done, no results");
                    None
                }
            };
            outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await.ok();

            // Try to sync files
            match backup.transfer_backup_files_to_filehost(DOMAIN_NAME).await {
                Ok(()) => {
                    // Emit the backup done event. This tells the filecontroler to sync backup files
                    // across all filehosts.
                    debug!("File transfer ok, indicating backup {:?} done via broadcast", version);
                    outbound.emit_backup_event(BackupEvent::new_done(DOMAIN_NAME, version)).await.ok();
                },
                Err(e) => error!("Error uploading backup files to filehost after manual backup: {}", e)
            }

            Ok(())
        },
        Err(e) => {
            let response = ErrorMessage { ok: false, code: Some(500), err: Some(e.to_string()) };
            outbound.respond(wrapper.delivery_info, response).await.ok();
            Err(e)
        }
    }
}

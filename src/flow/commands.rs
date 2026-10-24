use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::constantes::ROLE_MEDIA;
use crate::flow::transactions::FileManagerTransactionService;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::tracing::{info, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::v3::{ConfigService, DocumentEncryptionService, PkiService};
use serde::Deserialize;
use crate::external::mongo::CHAMP_FLAG_MEDIA_TRAITE;
use crate::external::mq::*;
use crate::flow::file_jobs::{get_batch_of_fichiersversion_leases, RequestLeaseForBatchOperation};

pub async fn process_command<M, E>(
    config: &dyn ConfigService,
    pki: &dyn PkiService,
    encryption: &E,
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated,
) -> Result<(), CommonError> where
    M: MongoDaoTyped,
    E: DocumentEncryptionService,
{
    let action = match wrapper.get_routing_action() {
        Some(action) => action,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err("No action provided in command")).await
    };

    match wrapper.message.kind {
        MessageKind::Commande => {
            match action {
                COMMAND_LEASE_FOR_IMAGE => command_lease_files_for_image(mongo, outbound, wrapper).await,
                COMMAND_LEASE_FOR_INDEX => command_lease_files_for_index(mongo, outbound, wrapper).await,
                COMMAND_LEASE_FOR_SUMMARY => command_lease_files_for_summary(mongo, outbound, wrapper).await,

                _ => {
                    warn!("Unknown action {} for process_command, skipping", action);
                    Ok(())
                }
            }
        },
        _ => {
            warn!("Unhandled message type with action {} for process_command, skipping", action);
            Ok(())
        }
    }
}

async fn command_lease_files_for_image<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let request: RequestLeaseForBatchOperation = wrapper.message.deserialize()?;

    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_MEDIA.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    }

    match get_batch_of_fichiersversion_leases(mongo, outbound, request, CHAMP_FLAG_MEDIA_TRAITE).await {
        Ok(response) => {
            // Encrypt response, it contains secret keys
            outbound.respond_encrypted(wrapper.delivery_info, response, wrapper.certificate.as_ref()).await
        },
        Err(e) => {
            info!("command_lease_files_for_image Error: {:?}", e);
            outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "No files available")).await
        }
    }
}

async fn command_lease_files_for_index<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    todo!()
}

async fn command_lease_files_for_summary<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    todo!()
}

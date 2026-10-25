use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::{Duration, Utc};
use millegrilles_common_rust::common_messages::RequeteDechiffrage;
use millegrilles_common_rust::constantes::*;
use crate::flow::transactions::FileManagerTransactionService;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::serde_json;
use millegrilles_common_rust::tracing::{info, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::v3::{ConfigService, DocumentEncryptionService, FormatService, MessagingService, PkiService};
use serde::Deserialize;
use crate::constants::*;
use crate::external::mongo::*;
use crate::external::mq::*;
use crate::flow::file_events::DelayedEventEmitterService;
use crate::flow::file_jobs::{get_batch_of_fichiersversion_leases, RequestLeaseForBatchOperation, get_batch_of_fichiersrep_leases, lease_batch_fichiersrep, set_flag_index_traite, return_fichiersrep_lease, RequestReturnFichiersrepLease, BackgroundJob};
use crate::models::TypeNode;

pub async fn process_command<M>(
    format: &dyn FormatService,
    messaging: &dyn MessagingService,
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated,
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let action = match wrapper.get_routing_action() {
        Some(action) => action,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err("No action provided in command")).await
    };

    match wrapper.message.kind {
        MessageKind::Commande => {
            match action {
                COMMAND_LEASE_FOR_IMAGE => command_lease_files_for_image(mongo, outbound, wrapper).await,
                COMMAND_LEASE_FOR_INDEX => command_lease_files_for_indexing(mongo, outbound, wrapper).await,
                COMMAND_LEASE_FOR_SUMMARY => command_lease_files_for_summary(mongo, outbound, wrapper).await,
                COMMAND_CONFIRMER_FICHIER_INDEXE => todo!("Obsolete?"),
                COMMAND_CONFIRM_INDEX => confirm_indexed_file(mongo, outbound, wrapper).await,
                COMMANDE_JOB_GET_KEY => job_get_key(format, messaging, mongo, outbound, wrapper).await,

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
            match response {
                Some(response) => outbound.respond_encrypted(wrapper.delivery_info, response, wrapper.certificate.as_ref()).await,
                None => outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "No files available")).await
            }
        },
        Err(e) => {
            info!("command_lease_files_for_image Error: {:?}", e);
            outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Server error")).await
        }
    }
}

async fn command_lease_files_for_indexing<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {

    let request: RequestLeaseForBatchOperation = wrapper.message.deserialize()?;

    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_SOLR_RELAI.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    }

    // CHAMP_FLAG_INDEX
    match get_batch_of_fichiersrep_leases(
        mongo,
        outbound,
        request,
        CHAMP_FLAG_INDEX,
        true
    ).await {
        Ok(response) => {
            match response {
                Some(response) => outbound.respond_encrypted(wrapper.delivery_info, response, wrapper.certificate.as_ref()).await,
                None => outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "No files available")).await
            }
        },
        Err(e) => {
            info!("command_lease_files_for_indexing Error: {:?}", e);
            outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "No files available")).await
        }
    }
}

async fn command_lease_files_for_summary<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {

    let request: RequestLeaseForBatchOperation = wrapper.message.deserialize()?;

    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_SOLR_RELAI.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    }

    let expiry = Utc::now() - Duration::minutes(5);
    let batch_size = request.batch_size.unwrap_or(5);
    // Iterate through files that have a flag_rag == false or undefined.
    let borrower = CHAMP_FLAG_SUMMARY;
    let filtre = doc!{
        FIELD_DELETED: false,
        CHAMP_TYPE_NODE: TypeNode::Fichier.to_str(),
        "$or": [
            {borrower: {"$exists": false}},
            {borrower: false},
        ]
    };
    match lease_batch_fichiersrep(mongo, outbound, &expiry, borrower, filtre, batch_size, true, true).await {
        Ok(Some(response)) => {
            outbound.respond_encrypted(wrapper.delivery_info, response, wrapper.certificate.as_ref()).await
        },
        Ok(None) => {
            outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "No files available")).await
        }
        Err(e) => {
            info!("command_lease_files_for_summary Error: {:?}", e);
            outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Server error")).await
        }
    }
}

// #[derive(Deserialize)]
// struct ParametresConfirmerJobIndexation {
//     job_id: String,
//     tuuid: String,
//     fuuid: Option<String>,
// }

async fn confirm_indexed_file<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: RequestReturnFichiersrepLease = wrapper.message.deserialize()?;

    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_SOLR_RELAI.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    }

    // set_flag_index_traite(mongo, command.tuuid.as_ref()).await?;
    return_fichiersrep_lease(mongo, command, CHAMP_FLAG_INDEX).await?;

    outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await
}

#[derive(Deserialize)]
struct CommandeGetJobKey {
    job_id: String,
    queue: String,
}

async fn job_get_key<M>(
    format: &dyn FormatService,
    messaging: &dyn MessagingService,
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: CommandeGetJobKey = wrapper.message.deserialize()?;

    if ! wrapper.certificate.verifier_roles_string(vec![
        ROLE_SOLR_RELAI.to_string(),
        ROLE_MEDIA.to_string(),
    ])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    }
    if ! wrapper.certificate.verifier_exchanges(vec![Securite::L2Prive, Securite::L3Protege])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    }

    let (collection_name, timeout) = match command.queue.as_str() {
        // "image" => (NOM_COLLECTION_IMAGES_JOBS, 180),
        "video" => (NOM_COLLECTION_VIDEO_JOBS, 600),
        // "index" => (NOM_COLLECTION_INDEXATION_JOBS, 180),
        _ => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "Unsupported message type")).await
    };

    let expired = Utc::now() - Duration::seconds(timeout);

    let collection = mongo.get_collection_typed::<BackgroundJob>(collection_name)?;
    let filtre = doc! {
        "job_id": &command.job_id,
        "$or": [
            {"etat": VIDEO_CONVERSION_ETAT_PENDING},
            {"etat": VIDEO_CONVERSION_ETAT_RUNNING, "date_maj": {"$lte": expired}},
        ]
    };
    let ops = doc! {
        "$set": {"etat": VIDEO_CONVERSION_ETAT_RUNNING},
        "$currentDate": {"date_maj": true, CHAMP_MODIFICATION: true}
    };
    let job = collection.find_one_and_update(filtre, ops).await?;

    match job {
        Some(inner) => {
            // Job exists. Request key from MaitreDesCles, redirect to requestor.
            let cle_id = inner.cle_id;
            let request = RequeteDechiffrage {
                domaine: DOMAIN_NAME.to_string(),
                liste_hachage_bytes: None,
                cle_ids: Some(vec![cle_id]),
                certificat_rechiffrage: Some(wrapper.certificate.chaine_pem()?),
                inclure_signature: None,
            };

            // Extract reply information, keymaster will reply directly
            let reply_properties = &wrapper.delivery_info.properties;
            let (reply_to, correlation_id) = match (reply_properties.reply_to(), reply_properties.correlation_id()) {
                (Some(reply_to), Some(correlation_id)) => (reply_to.to_string(), correlation_id.to_string()),
                _ => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "Missing reply information")).await
            };

            let routing = RoutageMessageAction::builder(
                DOMAINE_NOM_MAITREDESCLES,
                MAITREDESCLES_REQUETE_DECHIFFRAGE_V2,
                vec![Securite::L3Protege]
            )
                .reply_to(reply_to)
                .correlation_id(correlation_id)
                .blocking(false)
                .build();

            // The response is handled by KeyMaster. Emit a request (do not wait for response)
            let value = serde_json::to_value(request)?;
            let (response, _id) = format.build_action_message(
                MessageKind::Requete, &routing, value)?;
            messaging.emit(response, Some(routing)).await?;

            Ok(())
        },
        None => outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "Unknown job")).await
    }
}

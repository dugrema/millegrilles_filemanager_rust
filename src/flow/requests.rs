use std::collections::HashMap;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::common_messages::ResponseRequestDechiffrageV2Cle;
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage::{FormatChiffrage, optionformatchiffragestr};
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage_docs::EncryptedDocument;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::tracing::{debug, error, info};
use millegrilles_common_rust::v3::ConfigService;
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::chrono::serde::{ts_seconds, ts_milliseconds, ts_seconds_option};
use millegrilles_common_rust::millegrilles_cryptographie::serde_dates::mapstringepochseconds;
use serde::{Deserialize, Serialize};
use crate::constants::ROLE_USAGER;
use crate::external::mongo::*;
use crate::external::mongo_struct::{AudioDetail, FileComment, ImageDetail, MediaWebSubtitleDetail, NodeFichierRepOwned, SubtitleDetail, VideoDetail};
use crate::external::mq::*;
use crate::flow::directories::get_shared_tuuid;
use crate::flow::request_models::ResponseFichierRepVersion;

pub async fn process_request<M>(
    config: &dyn ConfigService,
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated,
) -> Result<(), CommonError> where
    M: MongoDaoTyped,
{
    let action = match wrapper.get_routing_action() {
        Some(action) => action,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err("No action provided in request")).await
    };

    match wrapper.message.kind {
        MessageKind::Requete => {
            match action {
                REQUEST_SYNC_DIRECTORY => request_sync_directory(mongo, outbound, wrapper).await,
                REQUETE_INFO_STATISTIQUES => request_statistics_info(mongo, outbound, wrapper).await,
                REQUETE_DOCUMENTS_PAR_TUUID => request_documents_by_tuuid(mongo, outbound, wrapper).await,

                _ => {
                    info!("Unknown action {} for process_request, skipping", action);
                    Ok(())
                }
            }
        },
        _ => {
            info!("Unhandled message type with action {} for process_request, skipping", action);
            Ok(())
        }
    }
}

#[derive(Deserialize)]
struct RequestSyncDirectory {
    cuuid: Option<String>,
    contact_id: Option<String>,
    last_sync: Option<i64>,
    skip: Option<i64>,
    limit_count: Option<i32>,
    // limit_size: Option<i32>,
    deleted: Option<bool>,
    produce_stats: Option<bool>,
}

#[derive(Serialize)]
struct RequestSyncDirectoryResponseFile {
    ok: bool,
    cuuid: Option<String>,
    stats: Option<Vec<ResultatStatistiquesRow>>,
    files: Vec<ResponseFichierRepVersion>,
    breadcrumb: Option<Vec<ResponseFichierRepVersion>>,
    keys: Option<Vec<ResponseRequestDechiffrageV2Cle>>,
    deleted_tuuids: Option<Vec<String>>,
    complete: bool,
}

async fn request_sync_directory<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    info!("Requesting sync directory");
    let request: RequestSyncDirectory = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };
    let (user_id, shared_tuuid) = match request.contact_id {
        None => (user_id, None),
        Some(contact_id) => {
            let cuuid = match request.cuuid.as_ref() {
                Some(inner) => inner.as_str(),
                None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(403, "Access refused, no tuuid for request with contact_id")).await
            };
            let share = get_shared_tuuid(mongo, user_id.as_str(), contact_id.as_str(), cuuid).await?;
            (share.user_id, share.tuuid)
        }
    };

    // Directory filter
    let deleted = request.deleted.unwrap_or(false);
    let cuuid = request.cuuid.clone();
    let filter = match deleted {
        false => match cuuid.as_ref() {
            Some(cuuid) => doc!{FIELD_PATH_CUUIDS_0: cuuid, FIELD_USER_ID: &user_id, FIELD_DELETED: false},
            None => doc!{FIELD_PATH_CUUIDS: {"$exists": false}, FIELD_USER_ID: &user_id, FIELD_DELETED: false}
        },
        true => match cuuid.as_ref() {
            Some(cuuid) => doc!{
                FIELD_PATH_CUUIDS_0: cuuid, FIELD_USER_ID: &user_id, FIELD_DELETED: true,
                "$or": [
                    {FIELD_NODE_TYPE: VALUE_NODE_TYPE_FICHIER, FIELD_FUUIDS_VERSION_0: {"$exists": true}},  // File has not been permanently deleted
                    {FIELD_NODE_TYPE: {"$ne": VALUE_NODE_TYPE_FICHIER}},                                    // Directory or Collection
                ]
            },
            None => doc!{
                FIELD_USER_ID: &user_id, FIELD_DELETED: true, FIELD_DELETED_INDIRECT: false,
                "$or": [
                    {FIELD_NODE_TYPE: VALUE_NODE_TYPE_FICHIER, FIELD_FUUIDS_VERSION_0: {"$exists": true}},  // File has not been permanently deleted
                    {FIELD_NODE_TYPE: {"$ne": VALUE_NODE_TYPE_FICHIER}},                                    // Directory or Collection
                ]
            }
        }
    };
    debug!("request_sync_directory Filter: {:?}", filter);

    let last_sync = match request.last_sync {
        Some(last_sync) => {
            match DateTime::from_timestamp(last_sync, 0) {
                Some(inner) => Some(inner),
                None => Err("request_sync_directory Invalid sync_date")?
            }
        },
        None => None
    };
    let skip = request.skip.unwrap_or(0);
    let produce_stats = skip == 0 || deleted || request.produce_stats == Some(true);

    todo!();

    Ok(())
}

#[derive(Deserialize)]
struct RequeteInfoStatistiques {
    /// Collection / repertoire a utiliser comme top de l'arborescence
    cuuid: Option<String>,
    contact_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct ResultatStatistiquesRow {
    #[serde(alias="_id")]
    type_node: String,
    taille: usize,
    count: usize,
}

#[derive(Serialize)]
struct ReponseInfoStatistiques {
    info: Vec<ResultatStatistiquesRow>,
}

async fn request_statistics_info<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    info!("Requesting sync directory");

    todo!();

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RequeteDocumentsParTuuids {
    tuuids_documents: Vec<String>,
    partage: Option<bool>,       // Flag qui indique qu'on utilise une permission (contact partage)
    contact_id: Option<String>,  // Identificateur de contact direct
}

#[derive(Clone, Serialize)]
struct ReponseDocumentsParTuuid {
    fichiers: Vec<ResponseFichierRepVersion>
}

async fn request_documents_by_tuuid<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    info!("Requesting sync directory");

    todo!();

    Ok(())
}

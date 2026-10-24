use std::collections::{HashMap, HashSet};
use millegrilles_common_rust::bson::{doc, Document};
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::common_messages::ResponseRequestDechiffrageV2Cle;
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage::{FormatChiffrage, optionformatchiffragestr};
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage_docs::EncryptedDocument;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::v3::ConfigService;
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::chrono::serde::{ts_seconds, ts_milliseconds, ts_seconds_option};
use millegrilles_common_rust::millegrilles_cryptographie::serde_dates::mapstringepochseconds;
use serde::{Deserialize, Serialize};
use crate::constants::{DOMAIN_NAME, ROLE_USAGER};
use crate::external::mongo::*;
use crate::external::mongo_struct::{AudioDetail, FileComment, ImageDetail, MediaWebSubtitleDetail, NodeFichierRepOwned, SubtitleDetail, VideoDetail};
use crate::external::mq::*;
use crate::flow::directories::{get_complete_files, get_directory_navigation_headers, get_shared_tuuid, ResultatStatistiquesRow};
use crate::flow::keys::convert_decrypted_key_into_v2;
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
                REQUEST_FILES_BY_TUUID => request_files_by_tuuid(mongo, outbound, wrapper).await,

                _ => {
                    warn!("Unknown action {} for process_request, skipping", action);
                    Ok(())
                }
            }
        },
        _ => {
            warn!("Unhandled message type with action {} for process_request, skipping", action);
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
    let (user_id, shared_tuuid) = match request.contact_id.clone() {
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
            Some(cuuid) => doc!{ FIELD_PATH_CUUIDS_0: cuuid, FIELD_USER_ID: &user_id, FIELD_DELETED: false },
            None => doc!{ FIELD_PATH_CUUIDS: None::<&str>, FIELD_USER_ID: &user_id, FIELD_DELETED: false }
        },
        true => match cuuid.as_ref() {
            Some(cuuid) => doc!{
                FIELD_PATH_CUUIDS_0: cuuid, FIELD_USER_ID: &user_id, FIELD_DELETED: true,
                "$or": [
                    { FIELD_NODE_TYPE: VALUE_NODE_TYPE_FICHIER, FIELD_FUUIDS_VERSION_0: {"$exists": true} },  // File has not been permanently deleted
                    { FIELD_NODE_TYPE: {"$ne": VALUE_NODE_TYPE_FICHIER} },                                    // Directory or Collection
                ]
            },
            None => doc!{
                FIELD_USER_ID: &user_id, FIELD_DELETED: true, FIELD_DELETED_INDIRECT: false,
                "$or": [
                    { FIELD_NODE_TYPE: VALUE_NODE_TYPE_FICHIER, FIELD_FUUIDS_VERSION_0: {"$exists": true} },  // File has not been permanently deleted
                    { FIELD_NODE_TYPE: {"$ne": VALUE_NODE_TYPE_FICHIER} },                                    // Directory or Collection
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

    debug!("Produce directory stats");
    let directory_info = match produce_stats {
        true => {
            let shared_collection = request.contact_id.is_some();
            let info = get_directory_navigation_headers(
                mongo,
                user_id.as_str(),
                filter.clone(),
                cuuid.as_ref(),
                last_sync,
                shared_collection,
                shared_tuuid,
                deleted,
            ).await?;
            Some(info)
        },
        false => None,
    };

    let limit_count = request.limit_count.unwrap_or(100);

    let (result, truncated) = get_complete_files(
        mongo,
        filter,
        last_sync.clone(),
        Some(skip),
        Some(limit_count)
    ).await?;
    let complete = !truncated && result.fichiers.len() < limit_count as usize;

    let (stats, breadcrumb, deleted_tuuids) = match directory_info {
        Some(info) => (info.stats, info.breadcrumb, info.deleted_tuuids),
        None => (None, None, None)
    };

    let mut sync_response = RequestSyncDirectoryResponseFile {
        ok: true,
        cuuid,
        stats,
        files: result.fichiers,
        breadcrumb,
        deleted_tuuids,
        keys: None,
        complete,
    };

    // Prepare decryption keys for client
    let mut keys = HashSet::new();
    for file in &sync_response.files {
        let file_keys = extract_key_ids_from_file(file)?;
        keys.extend(file_keys);
    }
    if let Some(breadcrumbs) = sync_response.breadcrumb.as_ref() {
        for breadcrumb in breadcrumbs {
            let breadcrumb_keys = extract_key_ids_from_file(breadcrumb)?;
            keys.extend(breadcrumb_keys);
        }
    }
    // Convert &String to String for request.
    if ! keys.is_empty() {
        let keys = keys.into_iter().map(|k| k.to_string()).collect();
        let key_response = outbound.get_keys(DOMAIN_NAME, keys, None).await?;
        let mut decryption_key_type = Vec::new();
        for k in key_response {
            decryption_key_type.push(convert_decrypted_key_into_v2(k)?);
        }
        sync_response.keys = Some(decryption_key_type);
    }

    outbound.respond(wrapper.delivery_info, sync_response).await
}

#[derive(Deserialize)]
struct RequeteInfoStatistiques {
    /// Collection / repertoire a utiliser comme top de l'arborescence
    cuuid: Option<String>,
    contact_id: Option<String>,
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
    info!("Requesting documents by tuuid");

    todo!();

    Ok(())
}

fn extract_key_ids_from_file(r: &ResponseFichierRepVersion) -> Result<Vec<&String>, CommonError> {
    // Use a set to dedupe key ids.
    let mut key_ids = HashSet::new();

    let fuuid = match r.fuuids_versions.as_ref() {
        Some(fuuids) => fuuids.get(0),
        None => None
    };

    if let Some(cle_id) = r.cle_id.as_ref() {
        key_ids.insert(cle_id);
    }
    if let Some(cle_id) = r.metadata.cle_id.as_ref() {
        key_ids.insert(cle_id);
    } else if let Some(ref_hachage_bytes) = r.metadata.ref_hachage_bytes.as_ref() {
        // Legacy method to get key id
        key_ids.insert(ref_hachage_bytes);
    } else if let Some(fuuid) = fuuid.as_ref() {
        // Legacy method to get key id
        key_ids.insert(fuuid);
    }
    if let Some(version) = r.version_courante.as_ref() {
        if let Some(cle_id) = version.cle_id.as_ref() {
            key_ids.insert(cle_id);
        }
    }

    Ok(key_ids.into_iter().collect())
}

#[derive(Deserialize)]
struct RequestFilesByTuuid {
    /// Tuuids to load
    tuuids: Vec<String>,
    /// Use this shared contact_id (more efficient than shared==true)
    shared_contact_id: Option<String>,
    /// Use true to allow loading from any shared collection
    shared: Option<bool>,
}

#[derive(Serialize)]
struct ResponseFilesByTuuids {
    files: Vec<ResponseFichierRepVersion>,
    keys: Option<Vec<ResponseRequestDechiffrageV2Cle>>,
}

async fn request_files_by_tuuid<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    info!("Requesting files by tuuid");
    let request: RequestFilesByTuuid = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

    let mut user_ids = HashSet::new();
    user_ids.insert(user_id.clone());
    let shared = request.shared_contact_id.is_some() || request.shared == Some(true);

    if shared {
        todo!();
    }

    let user_ids: Vec<String> = user_ids.into_iter().collect();
    let filter = doc! {
        FIELD_TUUID: { "$in": &request.tuuids },
        FIELD_USER_ID: { "$in": user_ids },
    };

    let (result, _) = get_complete_files(mongo, filter, None, None, None).await?;
    let files = result.fichiers;

    let mut response = ResponseFilesByTuuids { files, keys: None };
    let mut keys: HashSet<&String> = HashSet::new();
    for r in &response.files {
        let file_keys = extract_key_ids_from_file(r)?;
        keys.extend(file_keys);
    }
    // Convert &String to String for request.
    if ! keys.is_empty() {
        let keys = keys.into_iter().map(|k| k.to_string()).collect();
        let key_response = outbound.get_keys(DOMAIN_NAME, keys, None).await?;
        let mut decryption_key_type = Vec::new();
        for k in key_response {
            decryption_key_type.push(convert_decrypted_key_into_v2(k)?);
        }
        response.keys = Some(decryption_key_type);
    }

    outbound.respond(wrapper.delivery_info, response).await
}

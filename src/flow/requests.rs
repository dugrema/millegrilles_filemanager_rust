use crate::constants::*;
use crate::external::mongo::*;
use crate::external::mongo_struct::{ContactRow, RowPartagesUsager, StreamTokenRow};
use crate::external::mq::*;
use crate::flow::access::verify_user_certificate;
use crate::flow::directories::{ResultatStatistiquesRow, get_complete_files, get_directory_navigation_headers, get_directory_statistics, get_shared_tuuid};
use crate::flow::file_jobs::BackgroundJob;
use crate::flow::file_sharing::{get_contacts_user, get_shared_cuuids_by_userid, map_user_ids_nom_usager};
use crate::flow::keys::{FileDecryptionInformation, convert_decrypted_key_into_v2, convert_decrypted_key_vec_into_v2, fetch_decryption_information};
use crate::flow::request_models::*;
use millegrilles_common_rust::bson;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::serde::ts_seconds;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::common_messages::ResponseRequestDechiffrageV2Cle;
use millegrilles_common_rust::constantes::{DELEGATION_GLOBALE_PROPRIETAIRE, ROLE_SOLR_RELAI, RolesCertificats, Securite};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::{RoutageMessageAction, RoutingValidation};
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub async fn process_request<M>(
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
                REQUEST_FILES_BY_TUUID => request_files_by_tuuid(mongo, outbound, wrapper).await,
                REQUETE_JOBS_VIDEO => request_video_conversion_job(mongo, outbound, wrapper).await,
                REQUETE_PARTAGES_USAGER => user_shared_collections(mongo, outbound, wrapper).await,
                REQUETE_CHARGER_CONTACTS => user_shared_contacts(mongo, outbound, wrapper).await,
                REQUETE_PARTAGES_CONTACT => user_shared_collections_contacts(mongo, outbound, wrapper).await,
                REQUETE_SEARCH_INDEX_V2 => search_index(mongo, outbound, wrapper).await,
                REQUETE_GET_CLES_STREAM => get_streaming_keys(mongo, outbound, wrapper).await,

                // Obsolete?
                REQUETE_JWT_STREAMING => obsolete(outbound, wrapper).await,
                REQUETE_DOCUMENTS_PAR_TUUID => obsolete(outbound, wrapper).await,
                REQUETE_GET_CLES_FICHIERS => obsolete(outbound, wrapper).await,
                REQUETE_VERIFIER_ACCES_FUUIDS => obsolete(outbound, wrapper).await,
                REQUETE_VERIFIER_ACCES_TUUIDS => obsolete(outbound, wrapper).await,
                REQUETE_STRUCTURE_REPERTOIRE => obsolete(outbound, wrapper).await,
                REQUETE_SYNC_COLLECTION => obsolete(outbound, wrapper).await,
                REQUETE_SOUS_REPERTOIRES => obsolete(outbound, wrapper).await,
                REQUETE_RECHERCHE_INDEX => obsolete(outbound, wrapper).await,
                REQUETE_INFO_VIDEO => obsolete(outbound, wrapper).await,
                REQUETE_GET_CORBEILLE => obsolete(outbound, wrapper).await,
                REQUETE_SYNC_CORBEILLE => obsolete(outbound, wrapper).await,
                REQUETE_ACTIVITE_RECENTE => obsolete(outbound, wrapper).await,
                REQUETE_FAVORIS => obsolete(outbound, wrapper).await,
                _ => unknown(outbound, wrapper).await,
            }
        },
        _ => unknown(outbound, wrapper).await
    }
}

async fn obsolete(outbound: &MessageOutboundFacade, wrapper: MessageValidated) -> Result<(), CommonError> {
    warn!("Obsolete request called: {}", wrapper.delivery_info.routing_key);
    outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(410, "Obsolete")).await
}

async fn unknown(outbound: &MessageOutboundFacade, wrapper: MessageValidated) -> Result<(), CommonError> {
    warn!("Unknown request called: {}", wrapper.delivery_info.routing_key);
    outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "Unknown command")).await
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
    debug!("Requesting sync directory");
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

    outbound.respond_encrypted(wrapper.delivery_info, sync_response, wrapper.certificate.as_ref()).await
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
    debug!("request_statistics_info");
    let request: RequeteInfoStatistiques = wrapper.message.deserialize()?;
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(e) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, format!("Access denied: {:?}", e).as_str())).await
    };

    let allowed_cuuids = match request.contact_id.as_ref() {
        Some(contact_id) => {
            debug!("request_files_by_tuuid Allow any shared collection for user_id: {}", user_id);
            let allowed_cuuids = get_shared_cuuids_by_userid(
                mongo,
                user_id.as_str(),
                None,
                Some(contact_id)
            ).await?;
            Some(allowed_cuuids)
        },
        None => None
    };

    let mut filter = doc! {
        FIELD_DELETED: false,
        FIELD_DELETED_INDIRECT: false,
    };
    match allowed_cuuids {
        Some(cuuids) => {
            // Filter on either authenticated user_id (local) or allowed parent folders (cuuids)
            let mut vec_combined_checks = vec![
                doc! {"$or": [
                    // { FIELD_USER_ID: &user_id },
                    { FIELD_TUUID: { "$in": &cuuids } },
                    { CHAMP_PATH_CUUIDS: { "$in": &cuuids } }
                ]}
            ];
            if let Some(cuuid) = request.cuuid.as_ref() {
                // Filter on a specific directory
                vec_combined_checks.push(
                    doc! {"$or": [
                        doc!{ CHAMP_TUUID: &cuuid },
                        doc!{ CHAMP_PATH_CUUIDS: &cuuid }
                    ]}
                );
            }
            filter.insert("$and", bson::serialize_to_bson(&vec_combined_checks)?);
        },
        None => {
            // Filter on authenticated user_id
            filter.insert(FIELD_USER_ID, &user_id);
            if let Some(cuuid) = request.cuuid.as_ref() {
                // Filter on directory
                filter.insert(
                    "$or",
                    bson::serialize_to_bson(&vec![
                        doc!{ CHAMP_TUUID: &cuuid },
                        doc!{ CHAMP_PATH_CUUIDS: &cuuid }
                    ])?
                );
            }
        }
    }

    let resultat = get_directory_statistics(mongo, filter).await?;
    let response = ReponseInfoStatistiques { info: resultat };

    outbound.respond_encrypted(wrapper.delivery_info, response, wrapper.certificate.as_ref()).await
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
    debug!("Requesting files by tuuid");
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

    let allowed_cuuids = match shared {
        true => {
            debug!("request_files_by_tuuid Allow any shared collection for user_id: {}", user_id);
            let allowed_cuuids = get_shared_cuuids_by_userid(
                mongo,
                user_id.as_str(),
                None,
                request.shared_contact_id.as_ref()
            ).await?;
            Some(allowed_cuuids)
        },
        false => None
    };

    let mut filter = doc! { FIELD_TUUID: { "$in": &request.tuuids } };
    match allowed_cuuids {
        Some(cuuids) => {
            // Filter on either authenticated user_id (local) or allowed parent folders (cuuids)
            filter.insert("$or",  bson::serialize_to_bson(&vec![
                doc!{ FIELD_USER_ID: &user_id },
                doc!{ FIELD_TUUID: { "$in": &cuuids } },
                doc!{ FIELD_PATH_CUUIDS: { "$in": cuuids } },
            ])?);
        },
        None => {
            // Filter on authenticated user_id
            filter.insert(FIELD_USER_ID, &user_id);
        }
    }

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

    outbound.respond_encrypted(wrapper.delivery_info, response, wrapper.certificate.as_ref()).await
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RequeteJobsVideo {
    toutes_jobs: Option<bool>,
}

#[derive(Serialize)]
struct ResponseJobsVideo {
    jobs: Vec<BackgroundJob>,
}

async fn request_video_conversion_job<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    debug!("request_video_conversion_job");
    let request: RequeteJobsVideo = wrapper.message.deserialize()?;

    let mut flag_all_jobs = false;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if wrapper.certificate.verifier_delegation_globale(DELEGATION_GLOBALE_PROPRIETAIRE)? {
        flag_all_jobs = request.toutes_jobs.unwrap_or(false);
    } else if wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        // Pass
    } else {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    };

    let filter = match flag_all_jobs {
        true => doc! {},
        false => doc! { FIELD_USER_ID: &user_id }
    };

    let mut jobs = Vec::with_capacity(20);

    let collection = mongo.get_collection_typed::<BackgroundJob>(NOM_COLLECTION_VIDEO_JOBS)?;
    let mut cursor = collection.find(filter).await?;
    while let Some(row) = cursor.next().await {
        jobs.push(row?);
    }

    outbound.respond(wrapper.delivery_info, ResponseJobsVideo { jobs }).await
}

#[derive(Deserialize)]
struct RequetePartagesUsager { contact_id: Option<String> }

#[derive(Serialize)]
struct ReponsePartagesUsager {
    ok: bool,
    partages: Vec<RowPartagesUsager>,
    usagers: Option<Vec<ReponseUsager>>,
}

async fn user_shared_collections<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    debug!("user_shared_collections");
    let request: RequetePartagesUsager = wrapper.message.deserialize()?;
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(e) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, format!("Access denied: {:?}", e).as_str())).await
    };

    let mut filtre = doc! { CHAMP_USER_ID: &user_id };
    if let Some(inner) = request.contact_id.as_ref() {
        filtre.insert(CHAMP_CONTACT_ID, inner );
    }
    let collection = mongo.get_collection_typed::<RowPartagesUsager>(NOM_COLLECTION_PARTAGE_COLLECTIONS)?;
    let mut curseur = collection.find(filtre).await?;

    let mut partages = Vec::new();
    while let Some(row) = curseur.next().await {
        partages.push(row?);
    }

    let response = ReponsePartagesUsager { ok: true, partages, usagers: None };

    outbound.respond(wrapper.delivery_info, response).await
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct RequetePartagesContact {
    user_id: Option<String>
}

#[derive(Serialize)]
struct ReponseContact {
    #[serde(rename(deserialize = "contactId"))]
    contact_id: String,
    #[serde(rename(deserialize = "userId"))]
    user_id: String,
    #[serde(rename(deserialize = "nomUsager"))]
    nom_usager: String,
}

#[derive(Serialize)]
struct ReponseContacts {
    contacts: Vec<ReponseContact>,
}

async fn user_shared_contacts<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    debug!("user_shared_collections");
    let _request: RequetePartagesContact = wrapper.message.deserialize()?;
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(e) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, format!("Access denied: {:?}", e).as_str())).await
    };

    let mut user_ids = HashMap::new();
    let filtre = doc! { CHAMP_USER_ID: &user_id };
    let collection = mongo.get_collection_typed::<ContactRow>(NOM_COLLECTION_PARTAGE_CONTACT)?;
    let mut curseur = collection.find(filtre).await?;
    while let Some(row) = curseur.next().await {
        let row = row?;
        user_ids.insert(row.contact_user_id.clone(), row);
    }

    // Faire une requete aupres de MaitreDesComptes pour mapper le user_id avec le nom_usager courant
    let vec_user_ids: Vec<&str> = user_ids.keys().map(|u|u.as_str()).collect();
    let reponse_usagers = map_user_ids_nom_usager(outbound, &vec_user_ids).await?;

    // Mapper contact_id et nom_usager
    let mut contacts = Vec::new();
    for usager in reponse_usagers {
        let contact_id = match user_ids.remove(usager.user_id.as_str()) {
            Some(inner) => {
                inner.contact_id
            },
            None => {
                debug!("Mismatch user_id {}, SKIP", usager.user_id);
                continue;
            }
        };
        let contact = ReponseContact {
            contact_id,
            user_id: usager.user_id,
            nom_usager: usager.nom_usager,
        };
        contacts.push(contact);
    }

    let response = ReponseContacts { contacts };

    outbound.respond(wrapper.delivery_info, response).await
}


async fn user_shared_collections_contacts<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    debug!("user_shared_collections");
    let _request: RequetePartagesContact = wrapper.message.deserialize()?;
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(e) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, format!("Access denied: {:?}", e).as_str())).await
    };

    // Charger la liste des contact_id qui correspondent a l'usager courant
    let contacts = get_contacts_user(mongo, user_id.as_str()).await?;

    // Faire une requete pour obtenir tous les partages associes aux contacts
    let contact_ids: Vec<&str> = contacts.iter().map(|c| c.contact_id.as_str()).collect();
    let filtre = doc! { CHAMP_CONTACT_ID: { "$in": contact_ids } };
    let collection = mongo.get_collection_typed::<RowPartagesUsager>(NOM_COLLECTION_PARTAGE_COLLECTIONS)?;
    let mut curseur = collection.find(filtre).await?;

    let mut partages = Vec::new();
    while let Some(row) = curseur.next().await {
        partages.push(row?);
    }

    // Faire une requete pour obtenir l'information des usagers. Dedupe avec HashSet
    let mut user_ids = HashSet::new();
    user_ids.extend(partages.iter().map(|s| s.user_id.as_str()));
    let user_ids: Vec<&str> = user_ids.into_iter().collect();

    let usagers = map_user_ids_nom_usager(outbound, &user_ids).await?;

    let response = ReponsePartagesUsager { ok: true, partages, usagers: Some(usagers) };

    outbound.respond(wrapper.delivery_info, response).await
}


#[derive(Deserialize)]
struct RequestSearchIndexV2 {
    query: String,
    limit_count: Option<i64>,
    /// Size of the inital batch to return
    intitial_batch_size: Option<i64>,
    /// Top-level directory, search all children
    cuuid: Option<String>,
}

#[derive(Debug, Serialize)]
struct TransfertRequeteRechercheIndex {
    user_id: String,
    query: String,
    start: Option<i64>,
    limit: Option<i64>,
    cuuids_partages: Option<Vec<String>>,
    cuuid: Option<String>,
}

impl TransfertRequeteRechercheIndex {
    fn new_v2<S>(user_id: S, value: RequestSearchIndexV2) -> Self
    where S: ToString
    {
        Self {
            user_id: user_id.to_string(),
            query: value.query,
            start: Some(0),
            limit: value.limit_count,
            cuuids_partages: None,
            cuuid: value.cuuid,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SearchResultDocument {
    #[serde(rename = "id")]
    tuuid: String,
    user_id: String,
    fuuid: Option<String>,
    cuuids: Option<Vec<String>>,
    score: f64,
}

#[derive(Serialize, Deserialize)]
struct SearchResultContent {
    docs: Option<Vec<SearchResultDocument>>,
    #[serde(rename = "maxScore")]
    max_score: Option<f64>,
    #[serde(rename = "numFound")]
    num_found: Option<usize>,
    #[serde(rename = "numFoundExact")]
    num_found_exact: Option<bool>,
    start: Option<i64>,
}

#[derive(Deserialize)]
struct SearchResult {
    // ok: bool,
    resultat: SearchResultContent
}

#[derive(Serialize)]
struct RequestSearchIndexV2Response {
    ok: bool,
    files: Option<Vec<ResponseFichierRepVersion>>,
    keys: Option<Vec<ResponseRequestDechiffrageV2Cle>>,
    search_results: SearchResultContent,
}

async fn search_index<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    debug!("user_shared_collections");
    let request: RequestSearchIndexV2 = wrapper.message.deserialize()?;
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(e) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, format!("Access denied: {:?}", e).as_str())).await
    };

    let initial_batch_size = request.intitial_batch_size.unwrap_or(20) as usize;

    let mut requete_transfert = TransfertRequeteRechercheIndex::new_v2(
        &user_id, request);

    if requete_transfert.limit.is_none() {
        requete_transfert.limit = Some(200);
    }

    // Charger la liste des contact_id qui correspondent a l'usager courant
    let contacts = get_contacts_user(mongo, user_id.as_str()).await?;

    // Faire une requete pour obtenir tous les partages associes aux contacts
    let contact_ids: Vec<&str> = contacts.iter().map(|c| c.contact_id.as_str()).collect();
    let filtre = doc! {CHAMP_CONTACT_ID: {"$in": contact_ids}};
    let collection = mongo.get_collection_typed::<RowPartagesUsager>(
        NOM_COLLECTION_PARTAGE_COLLECTIONS)?;
    let mut curseur = collection.find(filtre).await?;

    // Conserver les tuuids (cuuids partages)
    let mut cuuids = HashSet::new();
    while let Some(row) = curseur.next().await {
        cuuids.insert(row?.tuuid.to_owned());
    }

    // Convertir en vec
    let cuuids: Vec<String> = cuuids.into_iter().collect();
    requete_transfert.cuuids_partages = Some(cuuids);

    let domaine: &str = RolesCertificats::SolrRelai.into();
    let routing = RoutageMessageAction::builder(domaine, REQUEST_SEARCH_FILES, vec![Securite::L3Protege])
        .timeout_blocking(5_000)
        .validation(RoutingValidation {
            ignore: false,
            exchanges: Some(vec![Securite::L3Protege]),
            domains: None,
            roles: Some(vec![ROLE_SOLR_RELAI.to_string()]),
        })
        .build();

    debug!("Searching index with parameters: {:?}", requete_transfert);
    let search_response: SearchResult = match outbound.send_request(routing, requete_transfert).await {
        Ok(response) => {
            match response.is_err()? {
                (true, err) => {
                    warn!("Error response from index search: {:?}", err);
                    return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, format!("Server error: {:?}", err).as_str())).await
                },
                _ => {
                    debug!("Search response: {:?}", response.message.contenu);
                    response.message.deserialize()?
                }
            }
        },
        Err(e) => {
            error!("Error from index search: {:?}", e);
            return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, format!("Server error: {:?}", e).as_str())).await
        }
    };

    let mut response = RequestSearchIndexV2Response {
        ok: true,
        files: None,
        keys: None,
        search_results: search_response.resultat,
    };

    if let Some(docs) = response.search_results.docs.as_ref () {
        if docs.len() > 0 {
            let first_batch_len = if docs.len() > initial_batch_size { initial_batch_size } else { docs.len() };
            // debug!("search_index_v2 Load first {} docs", first_batch_len);
            let first_batch = &docs[..first_batch_len];
            let tuuids: Vec<&String> = first_batch.iter().map(|d| &d.tuuid).collect();
            let filtre = doc!{ FIELD_TUUID: {"$in": &tuuids} };
            // debug!("search_index_v2 Filter for loading files:\n{:?}", filtre);
            let (result, _truncated) = get_complete_files(mongo, filtre, None, None, None).await?;
            // debug!("Loaded {} complete files", result.fichiers.len());
            response.files = Some(result.fichiers);

            // Retrieve decryption keys
            let mut cle_ids = HashSet::new();
            if let Some(files) = response.files.as_ref() {
                for r in files {
                    let keys = extract_key_ids_from_file(r)?;
                    cle_ids.extend(keys.iter().map(|k| k.to_string()));
                }
            }
            let cle_ids: Vec<String> = cle_ids.into_iter().collect();
            let keys = outbound.get_keys(DOMAIN_NAME, cle_ids, None).await?;
            let keys = convert_decrypted_key_vec_into_v2(keys)?;
            response.keys = Some(keys);
        } else {
            debug!("search_index_v2 No results in search");
        }
    }

    outbound.respond(wrapper.delivery_info, response).await
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParametresGetClesStream {
    /// Secret token (new)
    pub token: Option<String>,
    /// Requested keys
    pub fuuids: Vec<String>,
    // pub jwt: Option<String>,  // Old JWT token, obsolete
    // pub user_id: Option<String>,
}

#[derive(Serialize)]
struct ResponseGetStreamingKeys {
    ok: bool,
    keys: HashMap<String, FileDecryptionInformation>,
    #[serde(with="ts_seconds")]
    expiration: DateTime<Utc>,
}

async fn get_streaming_keys<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    debug!("user_shared_collections");
    let request: ParametresGetClesStream = wrapper.message.deserialize()?;

    if ! wrapper.certificate.verifier_roles(vec![RolesCertificats::Stream])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    }

    let token = match request.token {
        Some(token) => token,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(410, "Must use new token")).await
    };

    let collection_tokens = mongo.get_collection_typed::<StreamTokenRow>(NOM_COLLECTION_STREAM_TOKENS)?;
    let now = bson::datetime::DateTime::now();
    let filter = doc! { FIELD_TOKEN: &token, "expiration": { "$gte": now } };
    let token_info = match collection_tokens.find_one(filter).await? {
        Some(token_info) => token_info,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    };

    let mut allowed_fuuids_set = HashSet::new();
    allowed_fuuids_set.extend(token_info.fuuids);
    let mut requested_fuuids = HashSet::new();
    requested_fuuids.extend(request.fuuids.clone());

    if ! allowed_fuuids_set.is_superset(&requested_fuuids) {
        debug!("At least one requested fuuid is not allowed for token {}: {:?}", token, requested_fuuids);
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    }

    let keys = match fetch_decryption_information(
        mongo,
        outbound,
        token_info.user_id.as_str(),
        &request.fuuids
    ).await {
        Ok(keys) => keys,
        Err(e) => {
            error!("Error in fetch_decryption_information: {:?}", e);
            return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Error fetching information")).await
        }
    };

    let response = ResponseGetStreamingKeys {
        ok: true,
        keys,
        expiration: token_info.expiration,
    };
    outbound.respond_encrypted(wrapper.delivery_info, response, wrapper.certificate.as_ref()).await
}

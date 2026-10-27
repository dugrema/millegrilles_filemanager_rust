use crate::constants::*;
use crate::external::mongo::*;
use crate::external::mongo_struct::{MediaOwnedRow, NodeFichierRepOwned, NodeFichierVersionOwned};
use crate::external::mq::*;
use crate::flow::file_events::{claim_all_files, receive_visits};
use crate::flow::file_jobs::{BackgroundJob, BackgroundJobParams, RequestLeaseForBatchOperation, RequestReturnFichiersrepLease, get_batch_of_fichiersrep_leases, get_batch_of_fichiersversion_leases, lease_batch_fichiersrep, reset_indexation, return_fichiersrep_lease, sauvegarder_job_video};
use crate::flow::maintenance::RequeteGetVisitesFuuidsResponse;
use crate::models::TypeNode;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::{Duration, Utc};
use millegrilles_common_rust::common_messages::RequeteDechiffrage;
use millegrilles_common_rust::constantes::*;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::{MongoDaoImpl, MongoDaoTyped};
use millegrilles_common_rust::tracing::{error, info, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::v3::{FormatService, MessagingService};
use millegrilles_common_rust::{serde_json, tokio};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub async fn process_command(
    format: &dyn FormatService,
    messaging: &dyn MessagingService,
    mongo: Arc<MongoDaoImpl>,
    outbound: Arc<MessageOutboundFacade>,
    wrapper: MessageValidated,
) -> Result<(), CommonError> {
    let action = match wrapper.get_routing_action() {
        Some(action) => action,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err("No action provided in command")).await
    };

    match wrapper.message.kind {
        MessageKind::Commande => {
            match action {
                COMMAND_LEASE_FOR_IMAGE => command_lease_files_for_image(mongo.as_ref(), outbound.as_ref(), wrapper).await,
                COMMAND_LEASE_FOR_INDEX => command_lease_files_for_indexing(mongo.as_ref(), outbound.as_ref(), wrapper).await,
                COMMAND_LEASE_FOR_SUMMARY => command_lease_files_for_summary(mongo.as_ref(), outbound.as_ref(), wrapper).await,
                COMMAND_CONFIRM_INDEX => confirm_indexed_file(mongo.as_ref(), outbound.as_ref(), wrapper).await,
                COMMANDE_JOB_GET_KEY => job_get_key(format, messaging, mongo.as_ref(), outbound.as_ref(), wrapper).await,
                COMMANDE_VIDEO_TRANSCODER => video_transcoder(mongo.as_ref(), outbound.as_ref(), wrapper).await,
                COMMANDE_REINDEXER => reindex_files(mongo.as_ref(), outbound.as_ref(), wrapper).await,
                COMMAND_VISITS => command_visits(mongo.as_ref(), outbound.as_ref(), wrapper).await,
                COMMAND_CLAIM_ALL_FILES => command_claim_all_files(mongo, outbound, wrapper).await,

                // Obsolete - keeping to log applications that need to be fixed
                COMMANDE_RECLAMER_FUUIDS => obsolete(outbound.as_ref(), wrapper).await,
                COMMAND_CONFIRM_RAG => obsolete(outbound.as_ref(), wrapper).await,
                COMMAND_LEASE_FOR_RAG => obsolete(outbound.as_ref(), wrapper).await,
                COMMAND_CONFIRMER_FICHIER_INDEXE => obsolete(outbound.as_ref(), wrapper).await,

                _ => unknown(outbound.as_ref(), wrapper).await,
            }
        },
        _ => unknown(outbound.as_ref(), wrapper).await,
    }
}

async fn obsolete(outbound: &MessageOutboundFacade, wrapper: MessageValidated) -> Result<(), CommonError> {
    warn!("Obsolete command called: {}", wrapper.delivery_info.routing_key);
    outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(410, "Obsolete")).await
}

async fn unknown(outbound: &MessageOutboundFacade, wrapper: MessageValidated) -> Result<(), CommonError> {
    warn!("Unknown request called: {}", wrapper.delivery_info.routing_key);
    outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "Unknown command")).await
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandeVideoConvertir {
    pub tuuid: String,
    pub fuuid: String,
    pub mimetype: String,
    #[serde(rename="codecVideo")]
    pub codec_video: String,
    #[serde(rename="codecAudio")]
    pub codec_audio: String,
    #[serde(rename="resolutionVideo")]
    pub resolution_video: u32,
    #[serde(rename="qualityVideo")]
    pub quality_video: Option<i32>,
    #[serde(rename="bitrateVideo")]
    pub bitrate_video: Option<u32>,
    #[serde(rename="bitrateAudio")]
    pub bitrate_audio: u32,
    pub preset: Option<String>,
    pub audio_stream_idx: Option<i32>,
    pub subtitle_stream_idx: Option<i32>,
}

#[derive(Serialize)]
struct CommandeVideoConvertirReponse {
    ok: bool,
    job_id: Option<String>,
}

async fn video_transcoder<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: CommandeVideoConvertir = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

    let collection_reps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre_reps = doc!{ FIELD_TUUID: &command.tuuid, FIELD_USER_ID: &user_id, FIELD_DELETED: false };
    if collection_reps.find_one(filtre_reps).await?.is_none() {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
    }

    let filtre_fichier = doc! { FIELD_FUUID: &command.fuuid };
    let collection = mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;
    let version_courante = match collection.find_one(filtre_fichier).await {
        Ok(inner) => match inner {
            Some(inner) => inner,
            None => {
                info!("commande_video_convertir find_one : Fichier inconnu {}", command.fuuid);
                return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
            }
        },
        Err(e) => {
            error!("commande_video_convertir find_one : Erreur chargement/conversion {} : {:?}", command.fuuid, e);
            return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Server error")).await
        }
    };

    // Verifier si le fichier a deja un video correspondant
    let bitrate_quality = match &command.quality_video {
        Some(q) => q.to_owned(),
        None => match &command.bitrate_video {
            Some(b) => b.to_owned() as i32,
            None => 0,
        }
    };
    let mut cle_video = format!("{};{};{}p;{}", command.mimetype, command.codec_video, command.resolution_video, bitrate_quality);
    if let Some(inner) = command.audio_stream_idx.as_ref() {
        if *inner != 0 {
            cle_video = format!("{};a{}", cle_video, inner);
        }
    }
    if let Some(inner) = command.subtitle_stream_idx.as_ref() {
        cle_video = format!("{};s{}", cle_video, inner);
    }

    // Ensure no video exists with those parameters
    let filtre_media = doc!{ FIELD_FUUID: &command.fuuid, FIELD_USER_ID: &user_id };
    let collection_media = mongo.get_collection_typed::<MediaOwnedRow>(NOM_COLLECTION_MEDIA)?;
    if let Some(media) = collection_media.find_one(filtre_media).await? {
        if let Some(video) = media.video {
            if let Some(_) = video.get(cle_video.as_str()) {
                info!("commande_video_convertir Video file already exists with parameters {} for file {}", cle_video, command.fuuid);
                return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(409, "Converted file already exists")).await
            }
        }
    }

    // Conserver l'information de conversion, emettre nouveau message de job
    let (cle_id, format, nonce) = match (version_courante.cle_id, version_courante.format, version_courante.nonce) {
        (Some(cle_id), Some(format), Some(nonce)) => {
            (cle_id, format, nonce)
        },
        _ => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Decryption information missing")).await
    };

    let mimetype = version_courante.mimetype;
    let filehost_ids: Vec<&String> = version_courante.visites.keys().collect();

    let mut job = BackgroundJob::new(
        command.tuuid.as_str(),
        command.fuuid.as_str(),
        mimetype.as_str(),
        &filehost_ids.iter().map(|id| id.as_str()).collect::<Vec<&str>>(),
        cle_id.as_str(),
        format.into(),
        nonce.as_str()
    );
    job.user_id = Some(user_id.clone());
    let params = BackgroundJobParams {
        defaults: None,
        thumbnails: None,
        mimetype: Some(command.mimetype),
        codec_video: Some(command.codec_video),
        codec_audio: Some(command.codec_audio),
        resolution_video: Some(command.resolution_video),
        quality_video: command.quality_video,
        bitrate_video: command.bitrate_video,
        bitrate_audio: Some(command.bitrate_audio),
        preset: command.preset,
        audio_stream_idx: command.audio_stream_idx,
        subtitle_stream_idx: command.subtitle_stream_idx,
    };
    job.params = Some(params);
    sauvegarder_job_video(mongo, outbound, &job).await?;

    let response = CommandeVideoConvertirReponse { ok: true, job_id: Some(job.job_id) };
    outbound.respond(wrapper.delivery_info, response).await
}

async fn reindex_files<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    if wrapper.certificate.get_user_id()?.is_none() {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_delegation_globale(DELEGATION_GLOBALE_PROPRIETAIRE)? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not admin")).await
    };

    match reset_indexation(mongo, outbound).await {
        Ok(()) => outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await,
        Err(e) => outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, format!("Error: {:?}", e).as_str())).await,
    }
}

async fn command_claim_all_files(
    mongo: Arc<MongoDaoImpl>,
    outbound: Arc<MessageOutboundFacade>,
    wrapper: MessageValidated
) -> Result<(), CommonError> {
    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    if wrapper.certificate.get_user_id()?.is_none() {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_delegation_globale(DELEGATION_GLOBALE_PROPRIETAIRE)? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not admin")).await
    };

    let mongo_clone = mongo.clone();
    let outbound_clone = outbound.clone();
    let delivery_info_clone = wrapper.delivery_info.clone();

    // Spawn a new task that will handle the claim for all files
    tokio::spawn(async move {
        match claim_all_files(mongo_clone.as_ref(), outbound_clone.as_ref()).await {
            Ok(()) => outbound_clone.respond(delivery_info_clone, ErrorMessage::ok()).await,
            Err(e) => outbound_clone.respond(delivery_info_clone, ErrorMessage::err_code(500, format!("Error: {:?}", e).as_str())).await,
        }
    });

    // Return OK immediately. Any further feedback will require events.
    outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await
}

async fn command_visits<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: RequeteGetVisitesFuuidsResponse = wrapper.message.deserialize()?;

    // Authorization
    if ! wrapper.certificate.verifier_domaines(vec![DOMAINE_TOPOLOGIE.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    };

    match receive_visits(mongo, command).await {
        Ok(()) => outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await,
        Err(e) => {
            error!("Error processing command visits: {:?}", e);
            outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Server error")).await
        },
    }
}

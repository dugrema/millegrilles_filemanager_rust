use std::collections::HashMap;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::common_messages::ResponseRequestDechiffrageV2Cle;
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage::{FormatChiffrage, optionformatchiffragestr};
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage_docs::EncryptedDocument;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::tracing::info;
use millegrilles_common_rust::v3::ConfigService;
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::chrono::serde::{ts_seconds, ts_milliseconds, ts_seconds_option};
use millegrilles_common_rust::millegrilles_cryptographie::serde_dates::mapstringepochseconds;
use serde::{Deserialize, Serialize};
use crate::external::mongo_struct::{AudioDetail, FileComment, ImageDetail, MediaWebSubtitleDetail, NodeFichierRepOwned, SubtitleDetail, VideoDetail};
use crate::external::mq::*;

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
    files: Vec<ReponseFichierRepVersion>,
    breadcrumb: Option<Vec<ReponseFichierRepVersion>>,
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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FileCommentResponse {
    pub comment_id: String,
    pub encrypted_data: EncryptedDocument,
    #[serde(default, with="ts_seconds")]
    pub date: DateTime<Utc>,
    pub user_id: Option<String>,
}

impl From<FileComment> for FileCommentResponse {
    fn from(value: FileComment) -> Self {
        Self {
            comment_id: value.comment_id,
            encrypted_data: value.encrypted_data,
            date: value.date,
            user_id: value.user_id,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct ResponseVersionCourante {
    pub fuuid: String,
    pub mimetype: String,
    pub taille: u64,
    pub fuuids_reclames: Vec<String>,

    #[serde(with="mapstringepochseconds")]
    pub visites: HashMap<String, DateTime<Utc>>,

    // Mapping date
    #[serde(default, rename="_mg-derniere-modification", with="ts_seconds_option")]
    pub derniere_modification: Option<DateTime<Utc>>,

    // Champs optionnels media
    #[serde(skip_serializing_if="Option::is_none")]
    pub height: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub duration: Option<f32>,
    #[serde(rename="videoCodec", skip_serializing_if="Option::is_none")]
    pub video_codec: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub anime: Option<bool>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub images: Option<HashMap<String, ImageDetail>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub video: Option<HashMap<String, VideoDetail>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub audio: Option<Vec<AudioDetail>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub subtitles: Option<Vec<SubtitleDetail>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub web_subtitles: Option<Vec<MediaWebSubtitleDetail>>,

    // Information de chiffrage symmetrique (depuis 2024.3.0)
    #[serde(skip_serializing_if="Option::is_none")]
    pub cle_id: Option<String>,
    #[serde(default, with="optionformatchiffragestr", skip_serializing_if="Option::is_none")]
    pub format: Option<FormatChiffrage>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub nonce: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub verification: Option<String>,
}

#[derive(Clone, Serialize)]
pub struct ReponseFichierRepVersion {
    pub tuuid: String,
    pub user_id: String,
    pub type_node: String,
    pub supprime: bool,
    pub supprime_indirect: bool,
    pub metadata: DataChiffre,

    // Champs pour type_node Fichier
    #[serde(skip_serializing_if="Option::is_none")]
    pub mimetype: Option<String>,
    /// Fuuids des versions en ordre (plus recent en dernier)
    #[serde(skip_serializing_if="Option::is_none")]
    pub fuuids_versions: Option<Vec<String>>,

    // Champs pour type_node Fichiers/Repertoires
    /// Path des cuuids parents (inverse, parent immediat est index 0)
    #[serde(skip_serializing_if="Option::is_none")]
    pub path_cuuids: Option<Vec<String>>,

    // pub language: Option<String>,
    pub comments: Option<Vec<FileCommentResponse>>,
    pub tags: Option<Vec<EncryptedDocument>>,

    // Champs recuperes a partir de la version courante
    #[serde(skip_serializing_if="Option::is_none")]
    pub version_courante: Option<ResponseVersionCourante>,

    #[serde(default, skip_serializing_if="Option::is_none", with="ts_seconds_option")]
    pub derniere_modification: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if="Option::is_none", with="ts_seconds_option")]
    pub date_creation: Option<DateTime<Utc>>,

    // Information de chiffrage symmetrique (depuis 2024.3.0)
    #[serde(skip_serializing_if="Option::is_none")]
    pub cle_id: Option<String>,
    #[serde(default, with="optionformatchiffragestr", skip_serializing_if="Option::is_none")]
    pub format: Option<FormatChiffrage>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub nonce: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub verification: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub audio_stream_info: Option<Vec<AudioDetail>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub subtitle_stream_info: Option<Vec<SubtitleDetail>>,
}

impl From<NodeFichierRepOwned> for ReponseFichierRepVersion {
    fn from(value: NodeFichierRepOwned) -> Self {
        Self {
            tuuid: value.tuuid,
            user_id: value.user_id,
            type_node: value.type_node,
            supprime: value.supprime,
            supprime_indirect: value.supprime_indirect,
            metadata: value.metadata,
            mimetype: value.mimetype,
            fuuids_versions: value.fuuids_versions,
            path_cuuids: value.path_cuuids,
            comments: None,
            tags: None,
            version_courante: None,
            derniere_modification: value.derniere_modification,
            date_creation: value.date_creation,
            cle_id: None,
            format: None,
            nonce: None,
            verification: None,
            audio_stream_info: None,
            subtitle_stream_info: None,
        }
    }
}

#[derive(Clone, Serialize)]
struct ReponseDocumentsParTuuid {
    fichiers: Vec<ReponseFichierRepVersion>
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

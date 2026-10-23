use std::collections::HashMap;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::jwt_simple::prelude::Deserialize;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage::{FormatChiffrage, optionformatchiffragestr};
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage_docs::EncryptedDocument;
use millegrilles_common_rust::chrono::serde::{ts_seconds, ts_milliseconds, ts_seconds_option};
use millegrilles_common_rust::millegrilles_cryptographie::serde_dates::mapstringepochseconds;
use serde::Serialize;
use crate::external::mongo_struct::{AudioDetail, FileComment, ImageDetail, MediaWebSubtitleDetail, NodeFichierRepOwned, SubtitleDetail, VideoDetail};

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
pub struct ResponseFichierRepVersion {
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

impl From<NodeFichierRepOwned> for ResponseFichierRepVersion {
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

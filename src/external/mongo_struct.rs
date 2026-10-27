use std::collections::HashMap;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage::{FormatChiffrage, optionformatchiffragestr};
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage_docs::EncryptedDocument;
use millegrilles_common_rust::mongo_serde::map_chrono_datetime_as_bson_datetime;
use millegrilles_common_rust::bson::serde_helpers::datetime::FromChrono04DateTime;
use millegrilles_common_rust::dechiffrage::DataChiffre;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeFichierRepOwned {
    /// Identificateur unique d'un node pour l'usager
    pub tuuid: String,
    pub user_id: String,
    #[serde(rename="node_type")]
    pub type_node: String,
    #[serde(rename="deleted")]
    pub supprime: bool,
    #[serde(rename="deleted_indirect")]
    pub supprime_indirect: bool,
    pub metadata: DataChiffre,

    pub flag_index: bool,
    pub flag_rag: bool,
    pub flag_summary: bool,

    // Champs pour type_node Fichier
    pub mimetype: Option<String>,
    /// Fuuids des versions en ordre (plus recent en dernier)
    pub fuuids_versions: Option<Vec<String>>,

    // Champs pour type_node Fichiers/Repertoires
    /// Path des cuuids parents (inverse, parent immediat est index 0)
    pub path_cuuids: Option<Vec<String>>,

    // Mapping date - requis pour sync
    #[serde(with="FromChrono04DateTime")]
    pub created: DateTime<Utc>,
    #[serde(with="FromChrono04DateTime")]
    pub modified: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeFichierVersionOwned {
    pub fuuid: String,
    pub mimetype: String,
    pub taille: u64,

    pub tuuids: Vec<String>,
    pub fuuids_reclames: Vec<String>,

    pub language: Option<String>,

    #[serde(with="map_chrono_datetime_as_bson_datetime")]
    pub visites: HashMap<String, DateTime<Utc>>,
    #[serde(with="FromChrono04DateTime")]
    pub last_visit_verification: DateTime<Utc>,

    // Mapping date
    #[serde(default, with="FromChrono04DateTime")]
    pub created: DateTime<Utc>,
    #[serde(default, with="FromChrono04DateTime")]
    pub modified: DateTime<Utc>,

    #[serde(skip_serializing_if="Option::is_none")]
    pub flag_media: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub flag_media_retry: Option<i32>,
    pub flag_media_traite: bool,
    pub flag_video_traite: bool,

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

#[derive(Clone, Serialize, Deserialize)]
pub struct MediaOwnedRow {
    pub fuuid: String,
    pub user_id: String,

    // Mapping date
    #[serde(with="FromChrono04DateTime")]
    pub created: DateTime<Utc>,
    #[serde(with="FromChrono04DateTime")]
    pub modified: DateTime<Utc>,

    #[serde(skip_serializing_if="Option::is_none")]
    pub height: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub duration: Option<f32>,
    #[serde(rename="videoCodec", skip_serializing_if="Option::is_none")]
    pub video_codec: Option<String>,
    pub anime: bool,
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
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ImageDetail {
    pub hachage: String,
    pub mimetype: String,

    #[serde(skip_serializing_if="Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub height: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub taille: Option<u64>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub resolution: Option<u32>,

    #[serde(skip_serializing_if="Option::is_none")]
    pub data_chiffre: Option<String>,

    // Information dechiffrage - note : fuuid_v_courante du fichier -> ref_hachage_bytes
    #[serde(skip_serializing_if="Option::is_none")]
    pub header: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub nonce: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub cle_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct VideoDetail {
    pub fuuid: String,
    pub fuuid_video: String,
    pub taille_fichier: u64,
    pub mimetype: String,
    pub codec: String,

    /// Fix bug videas verticaux. Ajoute dans version 2023.7.4
    pub cle_conversion: Option<String>,

    // Metadata video transcode
    #[serde(skip_serializing_if="Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub height: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub bitrate: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub quality: Option<i32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub audio_stream_idx: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub subtitle_stream_idx: Option<u32>,

    // Information dechiffrage - note : fuuid -> ref_hachage_bytes
    #[serde(skip_serializing_if="Option::is_none")]
    pub header: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub nonce: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub cle_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AudioDetail {
    index: u32,
    title: Option<String>,
    language: Option<String>,
    codec_name: Option<String>,
    bit_rate: Option<u32>,
    default: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubtitleDetail {
    index: u32,
    language: Option<String>,
    title: Option<String>,
    codec_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MediaWebSubtitleDetail {
    pub language: String,
    pub fuuid: String,
    #[serde(skip_serializing_if="Option::is_none")]
    pub index: Option<i32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub label: Option<String>,
    // Encryption parameters
    pub cle_id: String,
    pub format: String,
    #[serde(skip_serializing_if="Option::is_none")]
    pub compression: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub nonce: Option<String>,
}

#[derive(Deserialize)]
pub struct CompleteFileRow {
    pub fichierrep: NodeFichierRepOwned,
    pub current_version: Option<NodeFichierVersionOwned>,
    pub media: Option<MediaOwnedRow>,
    pub comments: Option<Vec<FileComment>>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FileComment {
    /// File being referenced
    pub tuuid: String,
    /// Unique comment id - comes from transaction id
    pub comment_id: String,
    /// Encrypted comment data. Format is dict, e.g. {comment: "My comment"}
    pub encrypted_data: EncryptedDocument,
    #[serde(with="FromChrono04DateTime")]
    /// Transaction data
    pub date: DateTime<Utc>,
    /// User_id from the transaction certificate. None implies this is a system comment.
    pub user_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ContactRow {
    pub contact_id: String,
    pub user_id: String,
    pub contact_user_id: String
}

#[derive(Serialize, Deserialize)]
pub struct RowPartagesUsager {
    pub tuuid: String,
    pub user_id: String,
    pub contact_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct StreamTokenRow {
    pub token: String,
    pub fuuids: Vec<String>,
    #[serde(with="FromChrono04DateTime")]
    pub expiration: DateTime<Utc>,
    pub tuuid: String,
    pub user_id: String,
    pub contact_id: Option<String>,
}

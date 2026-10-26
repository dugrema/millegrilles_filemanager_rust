use std::collections::HashMap;
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage::FormatChiffrage;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage::optionformatchiffragestr;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage_docs::EncryptedDocument;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageMilleGrillesOwned;
use serde::{Deserialize, Serialize};
use crate::external::mongo_struct::{AudioDetail, ImageDetail, MediaWebSubtitleDetail, SubtitleDetail};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionNouvelleCollection {
    pub metadata: DataChiffre,
    pub cuuid: Option<String>,      // Insertion dans collection destination
    pub favoris: Option<bool>,
    // New fields used since 2026-10-23, avoids having to look-up the path of cuuids on replay
    pub cuuids: Option<Vec<String>>,
    pub user_id: Option<String>,    // When present, use this value instaad of certificate for user_id
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionNouvelleVersion {
    pub fuuid: String,
    pub cuuid: String,
    #[serde(skip_serializing_if="Option::is_none")]
    pub tuuid: Option<String>,  // uuid de la premiere commande/transaction comme collateur de versions
    pub mimetype: String,
    pub metadata: DataChiffre,
    pub taille: u64,

    // Valeurs de chiffrage symmetrique (depuis 2024.3)
    #[serde(skip_serializing_if="Option::is_none")]
    pub cle_id: Option<String>,
    #[serde(default, with="optionformatchiffragestr", skip_serializing_if="Option::is_none")]
    pub format: Option<FormatChiffrage>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub nonce: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub verification: Option<String>,

    // Full path cuuids, since 2026.7 (internal only)
    pub path_cuuids: Option<Vec<String>>,
    pub user_id: Option<String>,    // only allowed with re-signed transaction V2
}

pub fn is_mimetype_video<S>(mimetype: S) -> bool
where S: AsRef<str>
{
    const VIDEO_START: &'static str = "video/";
    const VIDEO_MIMETYPES: [&'static str; 5] = [
        "application/vnd.rn-realmedia",
        "application/vnd.rn-realplayer",
        "application/x-mplayer2",
        "application/x-shockwave-flash",
        "application/vnd.ms-asf"
    ];

    let mimetype_str = mimetype.as_ref();

    if mimetype_str.starts_with(VIDEO_START) {
        true
    } else {
        for mt in VIDEO_MIMETYPES {
            if mt == mimetype_str {
                return true
            }
        }
        false
    }
}

pub fn get_flags_media(mimetype: &str) -> (bool, bool, Option<String>, bool) {
    let mut flag_media_traite = true;
    let mut flag_video_traite = true;
    let mut flag_media = None;
    let mut flag_summary = true;

    // Information optionnelle pour accelerer indexation/traitement media
    if mimetype.starts_with("image/") {
        flag_media_traite = false;
        flag_media = Some("image".to_string());
        flag_summary = false;
    } else if is_mimetype_video(mimetype) {
        flag_media_traite = true;  // Thumbnails generes avec le video depuis 2024.9
        flag_video_traite = false;
        flag_media = Some("video".to_string());
    } else if mimetype == "application/pdf" {
        flag_media_traite = false;
        flag_media = Some("poster".to_string());
        flag_summary = false;
    } else if mimetype.starts_with("text/") {
        flag_summary = true;
    }
    (flag_media_traite, flag_video_traite, flag_media, flag_summary)
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TransactionAssocierConversions {
    // pub tuuid: Option<String>,
    pub fuuid: String,
    // pub user_id: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub mimetype: Option<String>,
    pub images: HashMap<String, ImageDetail>,
    pub anime: Option<bool>,
    pub duration: Option<f32>,
    #[serde(rename="videoCodec")]
    pub video_codec: Option<String>,
    pub audio: Option<Vec<AudioDetail>>,
    pub subtitles: Option<Vec<SubtitleDetail>>,

    // Since 2026.7, re-signing on reception linking to user_ids
    pub user_ids: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionDecrireFichier {
    pub tuuid: String,
    pub metadata: Option<DataChiffre>,
    pub mimetype: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionDecrireCollection {
    pub tuuid: String,
    pub metadata: Option<DataChiffre>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionUpdateFileTextContent {
    pub tuuid: String,
    // pub file_language: Option<String>,
    pub tags: Option<EncryptedDocument>,
    pub comment: Option<EncryptedDocument>,
    /// Comment_id allows to replace an existing comment
    pub comment_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionDeleteFileComment {
    pub tuuid: String,
    pub comment_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionAssocierVideo {
    pub tuuid: Option<String>,
    pub fuuid: String,
    pub user_id: String,
    pub mimetype: String,
    pub codec: String,
    pub fuuid_video: String,
    pub job_id: Option<String>,

    // Metadata video transcode
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub bitrate: Option<u32>,
    pub quality: Option<i32>,
    pub taille_fichier: u64,

    // Information dechiffrage - note : fuuid -> ref_hachage_bytes
    #[serde(skip_serializing_if="Option::is_none")]
    pub header: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub nonce: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub cle_id: Option<String>,

    /// Fix bug videas verticaux. Ajoute dans version 2023.7.4
    pub cle_conversion: Option<String>,

    #[serde(skip_serializing_if="Option::is_none")]
    pub audio_stream_idx: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub subtitle_stream_idx: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionSupprimerJobVideoV2 {
    pub tuuid: String,
    pub fuuid: String,
    pub job_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionDeleteV2 {
    pub command: MessageMilleGrillesOwned,
    pub directories: Option<Vec<String>>,
    pub subdirectories: Option<Vec<String>>,
    pub files: Option<Vec<String>>,
    pub user_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionDeleteV3 {
    /// Original user command (copy)
    pub command: Option<MessageMilleGrillesOwned>,
    /// Directories directly requested to be deleted
    pub directories: Option<Vec<String>>,
    /// Directories under deleted directories (so indirectly)
    pub subdirectories: Option<Vec<String>>,
    /// Files under deleted directories (so indirectly)
    pub subfiles: Option<Vec<String>>,
    /// Files directly requested to be deleted
    pub files: Option<Vec<String>>,
    /// Use that requested the operation
    pub user_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionSupprimerVideo {
    pub fuuid_video: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionSupprimerVideoV2 {
    pub fuuid_video: String,
    pub labels: Vec<String>,
    pub fuuid: String,
    pub user_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionMoveV2Directory {
    /// Reverse hierarchy of destination (cuuids)
    pub path: Vec<String>,
    /// List of individual cuuids to move
    pub directories: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionMoveV2 {
    pub command: MessageMilleGrillesOwned,
    pub destination: Vec<String>,
    pub directories: Option<Vec<TransactionMoveV2Directory>>,
    pub files: Option<Vec<String>>,
    pub user_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionFileSummary {
    pub fuuid: String,
    pub tuuid: String,
    /// Detected file language
    pub file_language: Option<String>,
    /// Generated tags
    pub tags: Option<EncryptedDocument>,
    /// The comment contains the summary
    pub comment: Option<EncryptedDocument>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionAddWebSubtitle {
    pub file_fuuid: String,
    pub user_id: Option<String>,
    pub subtitle_fuuid: String,
    pub language: String,
    pub index: Option<i32>,
    pub label: Option<String>,
    pub cle_id: String,
    // Encryption parameters
    pub format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<String>,
}

impl Into<MediaWebSubtitleDetail> for TransactionAddWebSubtitle {
    fn into(self) -> MediaWebSubtitleDetail {
        MediaWebSubtitleDetail {
            language: self.language,
            fuuid: self.subtitle_fuuid,
            index: self.index,
            label: self.label,
            cle_id: self.cle_id,
            format: self.format,
            compression: self.compression,
            nonce: self.nonce
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionRemoveWebSubtitle {
    pub file_fuuid: String,
    pub subtitle_fuuid: String,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionRecycleItemsV4 {
    /// Original command
    pub command: Option<MessageMilleGrillesOwned>,

    /// User Id
    pub user_id: String,

    /// Files to restore. Fuuids are needed to re-inject the tuuids in the versions collection.
    /// key = fuuid
    /// value = tuuid
    pub file_tuuids_by_fuuid: HashMap<String, Vec<String>>,

    /// Directories to restore.
    pub directory_tuuids: Vec<String>
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionSupprimerJobImageV2 {
    pub fuuid: String
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionAjouterContactLocal {
    /// Usager du carnet
    pub user_id: String,
    /// Contact local ajoute
    pub contact_user_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionSupprimerContacts {
    pub contact_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionPartagerCollections {
    pub cuuids: Vec<String>,
    pub contact_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionSupprimerPartageUsager {
    pub contact_id: String,
    pub tuuid: String,
}

use millegrilles_common_rust::constantes::*;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::{ChampIndex, IndexOptions, MongoDao};

pub const COLLECTION_NAME_REDOLOG: &str = "FileManager/redolog";
pub const COLLECTION_NAME_TRACKING: &str = "FileManager/tracking";

pub const INDEX_REDO_LOG_ID: &str = "redo_log_id";

// New fields
pub const FIELD_USER_ID: &str = "userId";
pub const FIELD_DATE_CREATED: &str = "created";
pub const FIELD_DATE_MODIFIED: &str = "modified";

// Old fields

pub const CHAMP_FUUID: &str = "fuuid";  // UUID fichier
pub const CHAMP_FUUIDS: &str = "fuuids";
pub const CHAMP_FUUIDS_RECLAMES: &str = "fuuids_reclames";
pub const CHAMP_FUUIDS_VERSIONS: &str = "fuuids_versions";
pub const CHAMP_TUUID: &str = "tuuid";  // UUID transaction initiale (fichier ou collection)
pub const CHAMP_TUUIDS: &str = "tuuids";
pub const CHAMP_CUUID: &str = "cuuid";  // UUID collection de tuuids
pub const CHAMP_CUUIDS: &str = "cuuids";  // Liste de cuuids (e.g. appartenance a plusieurs collections)
pub const CHAMP_CUUIDS_SUPPRIMES: &str = "cuuids_supprimes";  /// Liste de cuuids (e.g. appartenance a plusieurs collections)
pub const CHAMP_CUUIDS_SUPPRIMES_INDIRECT: &str = "cuuids_supprimes_indirect";  /// Liste de cuuids supprimes via parent
pub const CHAMP_SUPPRIME: &str = "supprime";
pub const CHAMP_SUPPRIME_INDIRECT: &str = "supprime_indirect";
pub const CHAMP_SUPPRIME_PATH: &str = "supprime_cuuids_path";
pub const CHAMP_ARCHIVE: &str = "archive";
pub const CHAMP_NOM: &str = "nom";
pub const CHAMP_METADATA: &str = "metadata";
pub const CHAMP_TITRE: &str = "titre";
pub const CHAMP_MIMETYPE: &str = "mimetype";
pub const CHAMP_FUUID_V_COURANTE: &str = "fuuid_v_courante";
pub const CHAMP_FAVORIS: &str = "favoris";
pub const CHAMP_TYPE_NODE: &str = "type_node";
// pub const CHAMP_FUUID_MIMETYPES: &str = "fuuidMimetypes";
pub const CHAMP_FLAG_INDEX: &str = "flag_index";
pub const CHAMP_FLAG_RAG: &str = "flag_rag";
pub const CHAMP_FLAG_SUMMARY: &str = "flag_summary";
pub const CHAMP_FLAG_INDEX_RETRY: &str = "index_retry";
pub const CHAMP_FLAG_INDEX_ERREUR: &str = "flag_index_erreur";
pub const CHAMP_INDEX_START: &str = "index_start";
pub const CHAMP_FLAG_INDEX_ETAT: &str = "etat";
pub const CHAMP_FLAG_MEDIA: &str = "flag_media";
pub const CHAMP_FLAG_MEDIA_TRAITE: &str = "flag_media_traite";
pub const CHAMP_FLAG_VIDEO_TRAITE: &str = "flag_video_traite";
pub const CHAMP_FLAG_MEDIA_RETRY: &str = "flag_media_retry";
pub const CHAMP_FLAG_MEDIA_ERREUR: &str = "flag_media_erreur";
pub const CHAMP_FLAG_DB_RETRY: &str = "retry";
pub const CHAMP_USER_ID: &str = "user_id";
pub const CHAMP_CLE_CONVERSION: &str = "cle_conversion";
pub const CHAMP_CONTACT_ID: &str = "contact_id";
pub const CHAMP_CONTACT_USER_ID: &str = "contact_user_id";
pub const CHAMP_PATH_CUUIDS: &str = "path_cuuids";
// pub const CHAMP_MAP_PATH_CUUIDS: &str = "map_path_cuuids";
// pub const CHAMP_CUUIDS_ANCETRES: &str = "cuuids_ancetres";
pub const CHAMP_TAILLE: &str = "taille";
pub const CHAMP_ETAT_JOB: &str = "etat";
pub const CHAMP_INSTANCES: &str = "instances";
pub const CHAMP_DATE_MAJ: &str = "date_maj";
pub const CHAMP_VISITES: &str = "visites";

pub const CONST_COMMENT_TAGS: &str = "tags";

pub const VIDEO_FLAG_CREER_THUMBNAILS: &str = "thumbnails";
pub const VIDEO_FLAG_DEFAULTS: &str = "defaults";
pub const VIDEO_FLAG_CLE_CONVERSION: &str = "cle_conversion";
// pub const VIDEO_PARAMS_CONVERSION_INITIAL: &str = "video/mp4;h264;270p;28";

pub const CONST_FIELD_LAST_VISIT_VERIFICATION: &str = "last_visit_verification";


pub const ERREUR_MEDIA_TOOMANYRETRIES: i32 = 1;

pub const MEDIA_RETRY_LIMIT: i32 = 5;
pub const MEDIA_IMAGE_BACTH_DEFAULT: i64 = 50;
pub const LIMITE_INDEXATION_BATCH: i64 = 1000;

pub const VIDEO_CONVERSION_ETAT_PENDING: i32 = 1;
pub const VIDEO_CONVERSION_ETAT_RUNNING: i32 = 2;
pub const VIDEO_CONVERSION_ETAT_PERSISTING: i32 = 3;
pub const VIDEO_CONVERSION_ETAT_ERROR: i32 = 4;
pub const VIDEO_CONVERSION_ETAT_ERROR_TOOMANYRETRIES: i32 = 5;

pub const VIDEO_CONVERSION_TIMEOUT_RUNNING: i32 = 10 * 60;  // Secondes
pub const VIDEO_CONVERSION_TIMEOUT_PERSISTING: i32 = 60 * 60;  // Secondes


pub async fn create_index_mongodb(db: &dyn MongoDao) -> Result<(), CommonError> {
    db.create_index(
        COLLECTION_NAME_REDOLOG,
        vec!(
            ChampIndex { nom_champ: String::from(TRANSACTION_CHAMP_ID), direction: 1 },
        ),
        Some(IndexOptions { nom_index: Some(String::from(INDEX_REDO_LOG_ID)), unique: true }),
    ).await?;

    db.create_index(
        COLLECTION_NAME_REDOLOG,
        vec!(
            ChampIndex { nom_champ: String::from(FIELD_PROCESSED), direction: 1 },
        ),
        Some(IndexOptions { nom_index: Some(String::from(INDEX_DATE_PROCESSED)), unique: false }),
    ).await?;

    db.create_index(
        COLLECTION_NAME_TRACKING,
        vec!(
            ChampIndex { nom_champ: String::from(FIELD_BID), direction: 1 },
        ),
        Some(IndexOptions { nom_index: Some(String::from(INDEX_BID)), unique: true }),
    ).await?;

    db.create_index(
        COLLECTION_NAME_TRACKING,
        vec!(
            ChampIndex { nom_champ: String::from(FIELD_DATE_PROCESSED), direction: 1 },
        ),
        Some(IndexOptions { nom_index: Some(String::from(INDEX_DATE_PROCESSED)), unique: false })
    ).await?;

    Ok(())
}

use millegrilles_common_rust::constantes::*;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::{ChampIndex, IndexOptions, MongoDao};

pub const COLLECTION_NAME_REDOLOG: &str = "FileManager/redolog";
pub const COLLECTION_NAME_TRACKING: &str = "FileManager/tracking";

pub const NOM_COLLECTION_FICHIERS_REP: &str = "FileManager/fichiersRep";
pub const NOM_COLLECTION_VERSIONS: &str = "FileManager/versionsFichiers";
pub const NOM_COLLECTION_MEDIA: &str = "FileManager/media";
// pub const NOM_COLLECTION_DOCUMENTS: &str = "FileManager/documents";
pub const NOM_COLLECTION_VIDEO_JOBS: &str = "FileManager/jobs/video";
pub const NOM_COLLECTION_PARTAGE_CONTACT: &str = "FileManager/partage/contacts";
pub const NOM_COLLECTION_PARTAGE_COLLECTIONS: &str = "FileManager/partage/collections";
pub const NOM_COLLECTION_QUOTAS_USAGERS: &str = "FileManager/quotas/usagers";
pub const NOM_COLLECTION_TEMP_VISITS: &str = "FileManager/temp/visits";
pub const NOM_COLLECTION_JOBS_LEASES: &str = "FileManager/jobs/leases";
pub const NOM_COLLECTION_JOBS_VERSIONS_LEASES: &str = "FileManager/jobs/versionsLeases";
pub const NOM_COLLECTION_FILE_COMMENTS: &str = "FileManager/fileComments";
pub const NOM_COLLECTION_STREAM_TOKENS: &str = "FileManager/streamTokens";


pub const INDEX_REDO_LOG_ID: &str = "redo_log_id";
pub const NOM_INDEX_ETAT_JOBS: &str = "etat_jobs_2";
pub const NOM_INDEX_USER_ID_TUUIDS: &str = "user_id_tuuids";
pub const NOM_INDEX_USER_ID: &str = "idx_user_id";

// New fields
pub const FIELD_USER_ID: &str = "user_id";
pub const FIELD_DELETED: &str = "deleted";
pub const FIELD_DELETED_INDIRECT: &str = "deleted_indirect";
pub const FIELD_DATE_CREATED: &str = "created";
pub const FIELD_DATE_MODIFIED: &str = "modified";
pub const FIELD_FUUID: &str = "fuuid";
pub const FIELD_TUUID: &str = "tuuid";
pub const FIELD_TUUIDS: &str = "tuuids";
pub const FIELD_CUUID: &str = "cuuid";
pub const FIELD_CUUIDS: &str = "cuuids";
pub const FIELD_PATH_CUUIDS: &str = "path_cuuids";
pub const FIELD_PATH_CUUIDS_0: &str = "path_cuuids.0";
pub const FIELD_NODE_TYPE: &str = "node_type";
pub const FIELD_FUUIDS_VERSIONS: &str = "fuuids_versions";
pub const FIELD_FUUIDS_VERSION_0: &str = "fuuids_versions.0";
pub const FIELD_MIMETYPE: &str = "mimetype";
pub const FIELD_METADATA: &str = "metadata";
pub const FIELD_TOKEN: &str = "token";

// Old fields

pub const CHAMP_FUUID: &str = FIELD_FUUID;  // UUID fichier
pub const CHAMP_FUUIDS: &str = "fuuids";
pub const CHAMP_FUUIDS_RECLAMES: &str = "fuuids_reclames";
pub const CHAMP_FUUIDS_VERSIONS: &str = FIELD_FUUIDS_VERSIONS;
pub const CHAMP_TUUID: &str = FIELD_TUUID;  // UUID transaction initiale (fichier ou collection)
pub const CHAMP_TUUIDS: &str = "tuuids";
pub const CHAMP_CUUID: &str = FIELD_CUUID;  // UUID collection de tuuids
pub const CHAMP_CUUIDS: &str = FIELD_CUUIDS;  // Liste de cuuids (e.g. appartenance a plusieurs collections)
pub const CHAMP_CUUIDS_SUPPRIMES: &str = "cuuids_supprimes";  /// Liste de cuuids (e.g. appartenance a plusieurs collections)
pub const CHAMP_CUUIDS_SUPPRIMES_INDIRECT: &str = "cuuids_supprimes_indirect";  /// Liste de cuuids supprimes via parent
pub const CHAMP_SUPPRIME: &str = FIELD_DELETED;
pub const CHAMP_SUPPRIME_INDIRECT: &str = FIELD_DELETED_INDIRECT;
pub const CHAMP_SUPPRIME_PATH: &str = "supprime_cuuids_path";
// pub const CHAMP_ARCHIVE: &str = "archive";
pub const CHAMP_NOM: &str = "nom";
pub const CHAMP_METADATA: &str = "metadata";
pub const CHAMP_TITRE: &str = "titre";
pub const CHAMP_MIMETYPE: &str = "mimetype";
pub const CHAMP_FUUID_V_COURANTE: &str = "fuuid_v_courante";
pub const CHAMP_FAVORIS: &str = "favoris";
pub const CHAMP_TYPE_NODE: &str = FIELD_NODE_TYPE;
// pub const CHAMP_FUUID_MIMETYPES: &str = "fuuidMimetypes";
pub const CHAMP_FLAG_INDEX: &str = "flag_index";
pub const CHAMP_FLAG_RAG: &str = "flag_rag";
pub const CHAMP_FLAG_SUMMARY: &str = "flag_summary";
pub const CHAMP_FLAG_INDEX_RETRY: &str = "index_retry";
pub const CHAMP_FLAG_INDEX_ERREUR: &str = "flag_index_erreur";
// pub const CHAMP_INDEX_START: &str = "index_start";
pub const CHAMP_FLAG_INDEX_ETAT: &str = "etat";
pub const CHAMP_FLAG_MEDIA: &str = "flag_media";
pub const CHAMP_FLAG_MEDIA_TRAITE: &str = "flag_media_traite";
pub const CHAMP_FLAG_VIDEO_TRAITE: &str = "flag_video_traite";
pub const CHAMP_FLAG_MEDIA_RETRY: &str = "flag_media_retry";
pub const CHAMP_FLAG_MEDIA_ERREUR: &str = "flag_media_erreur";
pub const CHAMP_FLAG_DB_RETRY: &str = "retry";
pub const CHAMP_USER_ID: &str = "user_id";
// pub const CHAMP_CLE_CONVERSION: &str = "cle_conversion";
pub const CHAMP_CONTACT_ID: &str = "contact_id";
pub const CHAMP_CONTACT_USER_ID: &str = "contact_user_id";
pub const CHAMP_PATH_CUUIDS: &str = "path_cuuids";
// pub const CHAMP_MAP_PATH_CUUIDS: &str = "map_path_cuuids";
// pub const CHAMP_CUUIDS_ANCETRES: &str = "cuuids_ancetres";
pub const CHAMP_TAILLE: &str = "taille";
// pub const CHAMP_ETAT_JOB: &str = "etat";
// pub const CHAMP_INSTANCES: &str = "instances";
pub const CHAMP_DATE_MAJ: &str = "date_maj";
pub const CHAMP_VISITES: &str = "visites";
pub const CHAMP_CLE_CONVERSION: &str = "cle_conversion";
pub const CHAMP_INSTANCES: &str = "instances";

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

pub const VALUE_NODE_TYPE_FICHIER: &str = "Fichier";

pub const VALUE_MEDIA_AUDIO: &str = "audio";
pub const VALUE_MEDIA_VIDEO: &str = "video";

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

    // Data tables

    // Fichiers Rep
    db.create_index(
        NOM_COLLECTION_FICHIERS_REP,
        vec!(
            ChampIndex {nom_champ: String::from("fuuids_versions"), direction: 1},
            ChampIndex {nom_champ: String::from("user_id"), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("fuuids_versions_user_id".to_string()), unique: false })
    ).await?;

    db.create_index(
        NOM_COLLECTION_FICHIERS_REP,
        vec!(
            ChampIndex {nom_champ: String::from("path_cuuids"), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("path_cuuids".to_string()), unique: false })
    ).await?;

    db.create_index(
        NOM_COLLECTION_FICHIERS_REP,
        vec!(
            ChampIndex {nom_champ: String::from("user_id"), direction: 1},
            ChampIndex {nom_champ: String::from("type_node"), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("user_id_type_node".to_string()), unique: false })
    ).await?;

    db.create_index(
        NOM_COLLECTION_FICHIERS_REP,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_TUUID), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("fichiers_tuuid".to_string()), unique: true })
    ).await?;

    db.create_index(
        NOM_COLLECTION_FICHIERS_REP,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_SUPPRIME), direction: -1},
            ChampIndex {nom_champ: String::from(CHAMP_SUPPRIME_INDIRECT), direction: -1},
            ChampIndex {nom_champ: String::from(CHAMP_MODIFICATION), direction: -1},
            ChampIndex {nom_champ: String::from(CHAMP_TUUID), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("fichiers_activite_recente_2".to_string()), unique: true })
    ).await?;

    db.create_index(
        NOM_COLLECTION_FICHIERS_REP,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_SUPPRIME), direction: -1},
            ChampIndex {nom_champ: String::from(CHAMP_FAVORIS), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("collections_favoris".to_string()), unique: false })
    ).await?;

    // Versions

    db.create_index(
        NOM_COLLECTION_VERSIONS,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_FUUID), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some(CHAMP_FUUID.to_string()), unique: true })
    ).await?;

    db.create_index(
        NOM_COLLECTION_VERSIONS,
        vec!(
            ChampIndex {nom_champ: String::from("fuuids_reclames"), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("fuuids_reclames".to_string()), unique: false })
    ).await?;

    db.create_index(
        NOM_COLLECTION_VERSIONS,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_FLAG_MEDIA_TRAITE), direction: 1},
            ChampIndex {nom_champ: String::from(CHAMP_CREATION), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("flag_media_traite".to_string()), unique: false })
    ).await?;

    db.create_index(
        NOM_COLLECTION_VERSIONS,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_FLAG_VIDEO_TRAITE), direction: 1},
            ChampIndex {nom_champ: String::from(CHAMP_CREATION), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("flag_video_traite".to_string()), unique: false })
    ).await?;

    db.create_index(
        NOM_COLLECTION_VERSIONS,
        vec!(
            ChampIndex {nom_champ: String::from(CONST_FIELD_LAST_VISIT_VERIFICATION), direction: 1},
            ChampIndex {nom_champ: String::from("visites.nouveau"), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("last_visits".to_string()), unique: false })
    ).await?;

    db.create_index(
        NOM_COLLECTION_VERSIONS,
        vec!(
            ChampIndex {nom_champ: String::from(FIELD_TUUIDS), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("tuuids".to_string()), unique: false })
    ).await?;

    // Video jobs

    db.create_index(
        NOM_COLLECTION_VIDEO_JOBS,
        vec!(
            ChampIndex {nom_champ: String::from(FIELD_TUUID), direction: 1},
            ChampIndex {nom_champ: String::from(CHAMP_CLE_CONVERSION), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("tuuid_params".to_string()), unique: true })
    ).await?;

    db.create_index(
        NOM_COLLECTION_VIDEO_JOBS,
        vec!(
            ChampIndex {nom_champ: String::from("job_id"), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("job_id".to_string()), unique: true })
    ).await?;

    db.create_index(
        NOM_COLLECTION_VIDEO_JOBS,
        vec!(
            ChampIndex {nom_champ: String::from("etat"), direction: 1},
            ChampIndex {nom_champ: String::from(CHAMP_MODIFICATION), direction: 1},
            ChampIndex {nom_champ: String::from(CHAMP_INSTANCES), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some(NOM_INDEX_ETAT_JOBS.to_string()), unique: false })
    ).await?;

    db.create_index(
        NOM_COLLECTION_VIDEO_JOBS,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_TUUID), direction: 1},
            ChampIndex {nom_champ: String::from(CHAMP_USER_ID), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some(NOM_INDEX_USER_ID_TUUIDS.to_string()), unique: false })
    ).await?;

    // User quotas

    db.create_index(
        NOM_COLLECTION_QUOTAS_USAGERS,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_USER_ID), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some(NOM_INDEX_USER_ID.to_string()), unique: true })
    ).await?;

    // Media

    db.create_index(
        NOM_COLLECTION_MEDIA,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_FUUID), direction: 1},
            ChampIndex {nom_champ: String::from(CHAMP_USER_ID), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("fuuid_userid".to_string()), unique: true })
    ).await?;

    // File sharing

    db.create_index(
        NOM_COLLECTION_PARTAGE_CONTACT,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_CONTACT_ID), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("contact_id".to_string()), unique: true })
    ).await?;

    db.create_index(
        NOM_COLLECTION_PARTAGE_COLLECTIONS,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_CONTACT_ID), direction: 1},
            ChampIndex {nom_champ: String::from(CHAMP_TUUID), direction: 1},
            ChampIndex {nom_champ: String::from(CHAMP_USER_ID), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("user_collection".to_string()), unique: true })
    ).await?;

    // Jobs

    db.create_index(
        NOM_COLLECTION_JOBS_LEASES,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_TUUID), direction: 1},
            ChampIndex {nom_champ: String::from(CHAMP_USER_ID), direction: 1},
            ChampIndex {nom_champ: String::from("borrower"), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("lease_id".to_string()), unique: true })
    ).await?;

    db.create_index(
        NOM_COLLECTION_JOBS_VERSIONS_LEASES,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_FUUID), direction: 1},
            ChampIndex {nom_champ: String::from("borrower"), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("lease_id".to_string()), unique: true })
    ).await?;

    db.create_index(
        NOM_COLLECTION_FILE_COMMENTS,
        vec!(
            ChampIndex {nom_champ: String::from(CHAMP_FUUID), direction: 1},
            ChampIndex {nom_champ: String::from("comment_id"), direction: 1},
        ),
        Some(IndexOptions { nom_index: Some("comment_id".to_string()), unique: true })
    ).await?;

    Ok(())
}

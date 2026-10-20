use millegrilles_common_rust::{base64_url, bson, chrono, openssl};
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::constantes::*;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::{ChampIndex, IndexOptions, MongoDao};
use millegrilles_common_rust::mongo_serde::option_chrono_04_datetime;
use millegrilles_common_rust::bson::serde_helpers::datetime::FromChrono04DateTime;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage_docs::EncryptedDocument;
use millegrilles_common_rust::tokio_stream::StreamExt;
use serde::{Deserialize, Serialize};

pub const COLLECTION_NAME_REDOLOG: &str = "FileManager/redolog";
pub const COLLECTION_NAME_TRACKING: &str = "FileManager/tracking";


pub const INDEX_REDO_LOG_ID: &str = "redo_log_id";

pub const FIELD_USER_ID: &str = "userId";

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

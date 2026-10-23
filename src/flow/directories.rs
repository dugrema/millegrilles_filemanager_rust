use millegrilles_common_rust::bson;
use millegrilles_common_rust::bson::{doc, Document};
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error};
use serde::{Deserialize, Serialize};
use crate::external::mongo::*;
use crate::external::mongo_struct::{ContactRow, NodeFichierRepOwned, RowPartagesUsager};
use crate::flow::request_models::ResponseFichierRepVersion;

pub struct UserSharedTuuid {
    pub user_id: String,
    pub tuuid: Option<String>,
}

pub async fn get_shared_tuuid<M>(
    mongo: &M,
    user_id: &str,
    contact_id: &str,
    cuuid: &str
) -> Result<UserSharedTuuid, CommonError> where M: MongoDaoTyped {
    // Determine the effective user_id by using the shared contact information
    let filtre = doc!{ CHAMP_CONTACT_ID: contact_id, CHAMP_CONTACT_USER_ID: user_id };
    let collection = mongo.get_collection_typed::<ContactRow>(NOM_COLLECTION_PARTAGE_CONTACT)?;
    let contact_row = match collection.find_one(filtre).await? {
        Some(inner) => inner,
        None => return Err(CommonError::Str("Access refused, invalid shared contact_id"))
    };

    let collection_reps_typed =
        mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let current_dir_filtre = doc!{ FIELD_TUUID: cuuid, FIELD_USER_ID: &contact_row.user_id };
    let directory = match collection_reps_typed.find_one(current_dir_filtre).await? {
        Some(inner) => inner,
        None => return Err(CommonError::String(format!("request_sync_directory Unknown directory {:?} for user {}", cuuid, user_id)))
    };

    let mut tuuids = vec![cuuid.to_owned()];
    if let Some(path_cuuids) = directory.path_cuuids {
        tuuids.extend(path_cuuids);
    }

    let filtre_shares = doc!{
        CHAMP_CONTACT_ID: contact_id,
        FIELD_TUUID: { "$in": tuuids }
    };
    let collection_shares = mongo.get_collection_typed::<RowPartagesUsager>(NOM_COLLECTION_PARTAGE_COLLECTIONS)?;
    let shared_collection = match collection_shares.find_one(filtre_shares).await? {
        Some(inner) => inner,
        None => return Err(CommonError::Str("request_sync_directory Access refused, the collection is not shared"))
    };

    Ok(UserSharedTuuid {
        user_id: contact_row.user_id,
        tuuid: Some(shared_collection.tuuid)
    })
}

pub struct DirectoryNavigationHeaders {
    pub stats: Option<Vec<ResultatStatistiquesRow>>,
    pub breadcrumb: Option<Vec<ResponseFichierRepVersion>>,
    pub deleted_tuuids: Option<Vec<String>>,
}

pub async fn get_directory_navigation_headers<M>(
    mongo: &M,
    user_id: &str,
    filter: Document,
    cuuid: Option<&String>,
    last_sync: Option<DateTime<Utc>>,
    shared_collection: bool,
    shared_tuuid: Option<String>,
    deleted: bool,
) -> Result<DirectoryNavigationHeaders, CommonError> where M: MongoDaoTyped {
    // This is an initial request. Fetch statistics for all files and direct sub-directories
    let stats = get_directory_statistics(mongo, filter.clone()).await?;

    // Load breadcrumb
    let breadcrumb = match cuuid.as_ref() {
        Some(cuuid) => {
            let collection_reps_typed =
                mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
            let current_dir_filtre = doc!{"tuuid": cuuid, "user_id": user_id};
            let current_dir = match collection_reps_typed.find_one(current_dir_filtre).await? {
                Some(inner) => inner,
                None => return Err(CommonError::Str("Unknown directory/access denied"))
            };

            // Add the current directory to path
            let path_cuuids = match current_dir.path_cuuids {
                Some(mut path_cuuids) => {
                    // Add the current directory to list (path_cuuids has reverse order)
                    path_cuuids.insert(0, cuuid.to_string());

                    if shared_collection {
                        // Filter out parent directories that are not shared
                        let mut shared_idx = 1;
                        if let Some(cuuid) = shared_tuuid.as_ref() {
                            for directory in &path_cuuids {
                                if directory.as_str() == cuuid.as_str() {
                                    break;
                                }
                                shared_idx += 1;
                            }
                        }
                        debug!("Cuuid {}, Truncating path_cuuids {:?} to {}", cuuid, path_cuuids, shared_idx);
                        path_cuuids.truncate(shared_idx);
                    }

                    debug!("Breadcrumb {:?}", path_cuuids);

                    path_cuuids
                }
                None => vec![cuuid.to_string()],
            };

            // Load tuuids from path
            let mut breadcrumb: Vec<ResponseFichierRepVersion> = Vec::new();
            let filtre_breadcrumb = doc!{"tuuid": {"$in": path_cuuids}, "user_id": &user_id};
            let mut cursor = collection_reps_typed.find(filtre_breadcrumb).await?;
            while cursor.advance().await? {
                let row = cursor.deserialize_current()?;
                breadcrumb.push(row.into());
            }

            Some(breadcrumb)
        }
        None => None
    };

    // Check if any tuuids were deleted since that last sync
    let mut deleted_tuuids = Vec::new();
    if let Some(sync_date) = last_sync {
        let sync_date_bson = bson::datetime::DateTime::from_chrono(sync_date);
        if ! deleted {
            let filtre = match cuuid.as_ref() {
                Some(cuuid) => doc! {
                        FIELD_PATH_CUUIDS_0: cuuid,
                        FIELD_USER_ID: &user_id,
                        FIELD_DATE_MODIFIED: doc!{"$gte": sync_date_bson},
                        FIELD_DELETED: true,
                    },
                None => doc! {
                        FIELD_PATH_CUUIDS: {"$exists": false},
                        FIELD_USER_ID: &user_id,
                        FIELD_DATE_MODIFIED: doc!{"$gte": sync_date_bson},
                        FIELD_DELETED: true,
                    }
            };
            let collection_reps_typed =
                mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
            let mut cursor_deleted = collection_reps_typed.find(filtre.clone()).await?;
            while cursor_deleted.advance().await? {
                let row = cursor_deleted.deserialize_current()?;
                deleted_tuuids.push(row.tuuid);
            }
        }
    }

    let deleted_tuuids = match deleted_tuuids.is_empty() { true => None, false => Some(deleted_tuuids)};

    Ok(DirectoryNavigationHeaders {
        stats: Some(stats),
        breadcrumb,
        deleted_tuuids,
    })
}

#[derive(Serialize, Deserialize)]
struct ResultatStatistiquesRow {
    #[serde(alias="_id")]
    type_node: String,
    taille: usize,
    count: usize,
}

async fn get_directory_statistics(
    mongo: &dyn MongoDao,
    filter: Document
) -> Result<Vec<ResultatStatistiquesRow>, CommonError> {
    let pipeline = vec![
        doc! { "$match": filter },
        doc! { "$project": {CHAMP_TYPE_NODE: 1, CHAMP_PATH_CUUIDS: 1, CHAMP_FUUIDS_VERSIONS: 1} },
        doc! { "$lookup": {
            "from": NOM_COLLECTION_VERSIONS,
            "localField": FIELD_FUUIDS_VERSIONS,
            "foreignField": FIELD_FUUID,
            "pipeline": [
                {"$project": {CHAMP_TAILLE: 1, FIELD_FUUID: 1}},
                { "$group": {
                        "_id": "$fuuid",
                        "taille": {"$sum": "$taille"}
                    }
                }
            ],
            "as": "versions",
        }},
        doc! { "$replaceRoot": { "newRoot": {"$mergeObjects": [ {"$arrayElemAt": ["$versions", 0] }, "$$ROOT" ]}}},
        doc! { "$group": {
                "_id": "$type_node",
                "taille": {"$sum": "$taille"},
                "count": {"$count": {}},
            }
        }
    ];

    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let mut cursor = collection.aggregate(pipeline).await?;
    let mut resultat: Vec<ResultatStatistiquesRow> = Vec::new();
    while let Some(d) = cursor.next().await {
        resultat.push(bson::deserialize_from_document(d?)?);
    }
    Ok(resultat)
}

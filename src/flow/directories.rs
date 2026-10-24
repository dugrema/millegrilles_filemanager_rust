use millegrilles_common_rust::{bson, serde_json};
use millegrilles_common_rust::bson::{doc, Document};
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error, warn};
use serde::{Deserialize, Serialize};
use crate::external::mongo::*;
use crate::external::mongo_struct::{CompleteFileRow, ContactRow, NodeFichierRepOwned, RowPartagesUsager};
use crate::flow::request_models::{ReponseDocumentsParTuuid, ResponseFichierRepVersion, ResponseVersionCourante};

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
pub struct ResultatStatistiquesRow {
    #[serde(alias="_id")]
    pub type_node: String,
    pub taille: usize,
    pub count: usize,
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
                "_id": "$node_type",
                "taille": {"$sum": "$taille"},
                "count": {"$count": {}},
            }
        }
    ];

    debug!("Directory statistics pipeline: {:?}", pipeline);

    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let mut cursor = collection.aggregate(pipeline).await?;
    let mut resultat: Vec<ResultatStatistiquesRow> = Vec::new();
    while let Some(d) = cursor.next().await {
        resultat.push(bson::deserialize_from_document(d?)?);
    }
    debug!("Directory stats results:\n{}", serde_json::to_string(&resultat)?);
    Ok(resultat)
}

/// Fetches complete file content from fichierrep, versions and media. Supports paging.
pub async fn get_complete_files(
    mongo: &dyn MongoDao,
    filtre: Document,
    changed_since: Option<DateTime<Utc>>,
    skip_count: Option<i64>,
    limit_count: Option<i32>
) -> Result<(ReponseDocumentsParTuuid, bool), CommonError> {
    let mut pipeline = vec![doc!{"$match": filtre}];

    if limit_count.is_some() || skip_count.is_some() {
        pipeline.push(doc!{"$sort": {FIELD_DATE_MODIFIED: 1}});  // Need to sort for paging
    }

    if let Some(skip) = skip_count {
        pipeline.push(doc! {"$skip": skip});
    }

    if changed_since.is_none() {
        // Limit before $lookup
        if let Some(limit) = limit_count {
            pipeline.push(doc! {"$limit": &limit});
        }
    }

    // Move all content under fichierrep doc for easier mapping to CompleteFileRow
    pipeline.push(doc!{"$replaceRoot": {"newRoot": {"fichierrep": "$$ROOT"}}});

    // Join version information
    pipeline.push(doc!{"$lookup": {
            "from": NOM_COLLECTION_VERSIONS,
            "localField": "fichierrep.fuuids_versions.0",
            "foreignField": FIELD_FUUID,
            "as": "versions",
        }}
    );
    pipeline.push(doc!{"$addFields": {
        "tuuid": "$fichierrep.tuuid",   // Used for troubleshooting deserialization errors
        "current_version": {"$arrayElemAt": ["$versions", 0]},
    }});
    pipeline.push(doc!{"$unset": "versions"});

    if let Some(changed_since) = changed_since {
        // Filter on changed date. Use max value for file between fichierrep and versions collections.
        pipeline.push(doc!{"$addFields": {"changed_since": {"$max": [
            format!("$fichierrep.{}", FIELD_DATE_MODIFIED),
            format!("$current_version.{}", FIELD_DATE_MODIFIED)]
        }}});
        pipeline.push(doc!{"$match": {"changed_since": {"$gte": changed_since}}});
        if let Some(limit) = limit_count {
            // Limit after match
            pipeline.push(doc! {"$limit": limit});
        }
    }

    // Join comments
    pipeline.push(doc!{"$lookup": {
            "from": NOM_COLLECTION_FILE_COMMENTS,
            "localField": "fichierrep.tuuid",
            "foreignField": FIELD_TUUID,
            "as": "comments",
        }}
    );

    // Add lookup to media content
    pipeline.push(
        doc!{"$lookup": {
            "from": NOM_COLLECTION_MEDIA,
            "localField": "fichierrep.fuuids_versions.0",
            "foreignField": FIELD_FUUID,
            "let": {"user_id": "$fichierrep.user_id"},  // Expose the outer user_id as inner variable $$user_id
            "pipeline": [{"$match": {"$expr": {"$eq": ["$user_id", "$$user_id"]}}}],        // Second key
            "as": "media_list",
        }}
    );
    pipeline.push(doc!{"$addFields": {"media": {"$arrayElemAt": ["$media_list", 0]}}});
    pipeline.push(doc!{"$unset": "media_list"});

    debug!("get_complete_files Pipeline: {:?}", pipeline);
    let collection_fichierrep = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;

    // DEBUG - output to collection
    // pipeline.push(doc!{"$out": {"db": middleware.get_database()?.name(), "coll": "GrosFichiers/TestSync"}});
    // collection_fichierrep.aggregate(pipeline, None).await?;

    let mut response = ReponseDocumentsParTuuid { fichiers: Vec::new() };
    let mut cursor = collection_fichierrep.aggregate(pipeline).await?;
    while cursor.advance().await? {
        let row = cursor.deserialize_current()?;
        let tuuid = row.get_str("tuuid").unwrap_or("UNKNOWN").to_string();
        let row: CompleteFileRow = match bson::deserialize_from_document(row) {
            Ok(inner) => inner,
            Err(e) => {
                warn!("get_complete_files Deserialization error on tuuid {}: {:?}", tuuid, e);
                continue    // Skip this entry
            }
        };

        let mut fichier_rep: ResponseFichierRepVersion = row.fichierrep.into();

        if let Some(comments) = row.comments {
            let mapped_comments = comments.into_iter().map(|c| c.into()).collect();
            fichier_rep.comments = Some(mapped_comments);
        }

        if let Some(version) = row.current_version {
            // Map the version to response format
            let mut version_response = ResponseVersionCourante {
                fuuid: version.fuuid,
                mimetype: version.mimetype,
                taille: version.taille,
                fuuids_reclames: version.fuuids_reclames,
                visites: version.visites,
                derniere_modification: Some(version.modified),
                height: None,
                width: None,
                duration: None,
                video_codec: None,
                anime: None,
                images: None,
                video: None,
                audio: None,
                subtitles: None,
                web_subtitles: None,
                cle_id: version.cle_id,
                format: version.format,
                nonce: version.nonce,
                verification: version.verification,
            };

            if let Some(media) = row.media {
                version_response.anime = Some(media.anime);
                version_response.height = media.height;
                version_response.width = media.width;
                version_response.duration = media.duration;
                version_response.video_codec = media.video_codec;
                version_response.images = media.images;
                version_response.video = media.video;
                version_response.audio = media.audio;
                version_response.subtitles = media.subtitles;
                version_response.web_subtitles = media.web_subtitles;
            }

            fichier_rep.version_courante = Some(version_response);
        }
        response.fichiers.push(fichier_rep);
    }

    let truncated = false;

    Ok((response, truncated))
}

#[derive(Deserialize)]
pub struct RowRepertoirePaths {
    pub path_cuuids: Option<Vec<String>>,
}

pub async fn get_path_cuuid(
    mongo: &dyn MongoDao,
    cuuid: &str
) -> Result<Option<Vec<String>>, CommonError> {
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre = doc! { CHAMP_TUUID: cuuid };
    let doc_parent: RowRepertoirePaths = match collection
        .find_one(filtre)
        .projection(doc!{CHAMP_TUUID: 1, CHAMP_CUUID: 1, CHAMP_PATH_CUUIDS: 1})
        .await?
    {
        Some(inner) => bson::deserialize_from_document(inner)?,
        None => return Ok(None)
    };

    // Build path with parent first
    let path_cuuids = match doc_parent.path_cuuids {
        Some(mut inner) => {
            inner.insert(0, cuuid.to_owned());
            inner
        },
        None => vec![cuuid.to_owned()]
    };

    Ok(Some(path_cuuids))
}

use crate::external::mongo::*;
use crate::external::mongo_struct::{CompleteFileRow, ContactRow, NodeFichierRepOwned, RowPartagesUsager};
use crate::flow::file_topology::verifier_visites_topologies;
use crate::flow::request_models::{ReponseDocumentsParTuuid, ResponseFichierRepVersion, ResponseVersionCourante};
use crate::flow::transaction_models::TransactionMoveV2Directory;
use crate::models::TypeNode;
use millegrilles_common_rust::bson::{Document, doc};
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::mongodb::ClientSession;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error, warn};
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::{bson, serde_json};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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

pub async fn get_directory_statistics(
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

pub struct ParseSelectionDirectoriesResult {
    pub destination_path: Vec<String>,
    pub files: Option<Vec<String>>,
    pub directories: Option<Vec<TransactionMoveV2Directory>>,
}

#[derive(Clone, Debug)]
struct NodeInformation {
    tuuid: String,
    parent: Option<String>,
    destination_path: Option<Vec<String>>,
}

struct SubdirectoryList {
    node_information: NodeInformation,
    children: Vec<String>,
}

pub async fn parse_selection_directories<M>(
    mongo: &M,
    user_id: &str,
    destination_cuuid: &str,
    tuuids: &Vec<String>,
    keep_deleted: bool,
    include_indirectly_deleted: bool,
    session: &mut ClientSession,
) -> Result<ParseSelectionDirectoriesResult, CommonError> where M: MongoDaoTyped {

    let collection_fichierrep =
        mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;

    // Get the destination path. This will replace the path of all files/directories being moved.
    // It will be used to update the path of subdirectories and their files.
    let destination_path = {
        let collection = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
        let filtre = doc!{"tuuid": destination_cuuid};
        let destination_directory = match collection
            .find_one(filtre)
            .session(&mut *session)
            .await?
        {
            Some(inner) => inner,
            None => Err("Unknown destination directory")?
        };
        let mut destination_path = vec![destination_cuuid.to_owned()];
        if let Some(path_cuuids) = destination_directory.path_cuuids {
            destination_path.extend(path_cuuids.iter().map(|s| s.to_string()))
        }
        destination_path
    };

    debug!("commande_ajouter_fichiers_collection Destination path: {:?}", destination_path);

    let filtre = doc!{ FIELD_TUUID: {"$in": tuuids} };
    let mut cursor = collection_fichierrep
        .find(filtre)
        .session(&mut *session)
        .await?;
    let mut files = Vec::new();
    let mut tuuids_by_cuuid: HashMap<String, Vec<String>> = HashMap::new();

    let mut directories: Vec<NodeInformation> = Vec::new();

    while let Some(row) = cursor.next(&mut *session).await {
        let row = row?;
        let parent = match row.path_cuuids.as_ref() {
            Some(inner) => match inner.get(0) {
                Some(inner) => Some(inner.to_string()),
                None => None
            },
            None => None
        };
        let node_info = NodeInformation { tuuid: row.tuuid.to_owned(), parent, destination_path: Some(destination_path.clone()) };
        if row.user_id != user_id {
            warn!("commande_ajouter_fichiers_collection File with wrong user_id, SKIPPING");
            continue
        }
        if row.type_node == TypeNode::Fichier.to_str() {
            files.push(node_info);
        } else {
            directories.push(node_info);
        }

        // Map all tuuids for post-transaction cleanup (search index, events)
        let cuuid = match row.path_cuuids.as_ref() {
            Some(path_cuuids) => match path_cuuids.get(0) {
                Some(cuuid) => cuuid.as_str(),
                None => "root",
            },
            None => "root"
        };
        match tuuids_by_cuuid.get_mut(cuuid) {
            Some(tuuids) => tuuids.push(row.tuuid.to_owned()),
            None => {
                tuuids_by_cuuid.insert(cuuid.to_owned(), vec![row.tuuid.to_owned()]);
            }
        }
    }

    // Make a list of all subdirectories to move. Include all subdirectories (deleted or not).
    let cuuids: Vec<&String> = directories.iter().map(|d| &d.tuuid).collect();
    let mut filtre = doc!{
        "path_cuuids": {"$in": &cuuids},
        "type_node": TypeNode::Repertoire.to_str()
    };
    if keep_deleted {
        // No filter to add - just keep all files whether they are deleted or not
    } else if include_indirectly_deleted {
        // Ignore files that were directly deleted (supprime == true && supprime_indirect == false)
        // This is used to copy files out from the Trash area
        filtre.insert("$or", vec![
            doc!{ FIELD_DELETED: false },
            doc!{ FIELD_DELETED_INDIRECT: true },  // Implies supprime == true
        ]);
    } else {
        // keep_deleted is false
        filtre.insert(FIELD_DELETED, false);
    }
    let mut cursor = collection_fichierrep
        .find(filtre)
        .session(&mut *session)
        .await?;
    while let Some(row) = cursor.next(&mut *session).await {
        let row = row?;
        if row.user_id != user_id {
            warn!("commande_ajouter_fichiers_collection Directory with wrong user_id, SKIPPING");
            continue
        }
        // subdirectories.push(row.tuuid.to_owned());
        let parent = match row.path_cuuids.as_ref() {
            Some(inner) => match inner.get(0) {
                Some(inner) => Some(inner.to_string()),
                None => None
            },
            None => None
        };
        let node_info = NodeInformation { tuuid: row.tuuid.to_owned(), parent, destination_path: None };
        directories.push(node_info);

        // Add all directory tuuids for post-transaction cleanup (search index, events)
        let cuuid = match row.path_cuuids.as_ref() {
            Some(path_cuuids) => match path_cuuids.get(0) {
                Some(cuuid) => cuuid.as_str(),
                None => "root",
            },
            None => "root"
        };
        match tuuids_by_cuuid.get_mut(cuuid) {
            Some(tuuids) => tuuids.push(row.tuuid.to_owned()),
            None => {
                tuuids_by_cuuid.insert(cuuid.to_owned(), vec![row.tuuid.to_owned()]);
            }
        }
    }

    debug!("commande_ajouter_fichiers_collection Directories list: {:?}", directories);

    let directories_by_tuuid = {
        let mut directories_by_tuuid = HashMap::new();

        // Create destination node
        directories_by_tuuid.insert(
            destination_cuuid.to_owned(),
            NodeInformation { tuuid: destination_cuuid.to_owned(), parent: None, destination_path: None }
        );

        for directory in &directories {
            directories_by_tuuid.insert(directory.tuuid.to_owned(), directory.clone());
        }
        directories_by_tuuid
    };

    debug!("commande_ajouter_fichiers_collection Directories by tuuid: {:?}", directories_by_tuuid);

    // Fill in the destination path for each node
    for current_node in directories.iter_mut() {
        let mut node_destination_path = vec![];
        let mut depth = 0;

        // Start parent at current node
        let mut parent_node = directories_by_tuuid.get(&current_node.tuuid).expect("get node");
        loop {
            if depth > 100 {
                warn!("commande_ajouter_fichiers_collection Ininite loop detected on path, skipping tuuid: {}", current_node.tuuid);
                break;
            }
            depth += 1;

            match parent_node.destination_path.as_ref() {
                Some(path) => {
                    node_destination_path.extend(path.iter().map(|p| p.to_string()));
                    current_node.destination_path = Some(node_destination_path);
                    break  // Done
                },
                None => {
                    match parent_node.parent.as_ref() {
                        Some(parent) => {
                            node_destination_path.push(parent.to_string());
                            match directories_by_tuuid.get(parent) {
                                Some(ancestor) => {
                                    // Keep looping
                                    parent_node = ancestor;
                                },
                                None => {
                                    warn!("commande_ajouter_fichiers_collection No matching parent node, ignoring tuuid: {}", current_node.tuuid);
                                    break  // Error, no matching parent node. Ignore this node.
                                }
                            }
                        },
                        None => {
                            node_destination_path.extend(destination_path.clone());
                            current_node.destination_path = Some(node_destination_path);
                            break  // Done, no more parents this is the destination
                        }
                    }
                }
            }
        }
    }

    // Build list of moves
    let mut directories_by_tuuid = HashMap::new();
    directories_by_tuuid.insert(destination_cuuid.to_owned(), SubdirectoryList { node_information: NodeInformation {
        tuuid: destination_cuuid.to_owned(),
        parent: None,
        destination_path: Some((&destination_path[1..]).to_owned()),
    }, children: vec![]} );
    for directory in &directories {
        directories_by_tuuid.insert(directory.tuuid.to_owned(), SubdirectoryList { node_information: directory.clone(), children: vec![]} );
    }

    for directory in &directories {
        if let Some(parent) = directory.parent.as_ref() {
            match directories_by_tuuid.get_mut(parent) {
                Some(parent) => {
                    parent.children.push(directory.tuuid.clone());
                },
                None => {
                    // Unkonwn parent, add to destination
                    let destination = directories_by_tuuid.get_mut(destination_cuuid).expect("get_mut destination");
                    destination.children.push(directory.tuuid.clone());
                }
            }
        }
    }

    let directories = match directories_by_tuuid.len() {
        0 => None,
        _ => {
            let mut directories_move = Vec::new();
            for directory in directories_by_tuuid.into_values() {
                match directory.node_information.destination_path {
                    Some(mut destination) => {
                        if directory.children.len() == 0 {
                            continue  // No subdirectory, Skip
                        }
                        destination.insert(0, directory.node_information.tuuid);  // Push cuuid in front as parent for subfolders
                        directories_move.push(
                            TransactionMoveV2Directory {
                                path: destination,
                                directories: directory.children
                            }
                        );
                    }
                    None => {
                        warn!("Unable to move tuuid {}, no parent path", directory.node_information.tuuid);
                    }
                }
            }
            match directories_move.len() {
                0 => None,
                _ => Some(directories_move)
            }
        }
    };

    let files = match files.len() {
        0 => None,
        _ => Some(files.into_iter().map(|s| s.tuuid).collect())
    };

    Ok(ParseSelectionDirectoriesResult {destination_path, files, directories})
}

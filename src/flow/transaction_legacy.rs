// Support for older transactions no longer used through commands
// Needed for restoration of backups (redo-log).

use crate::external::mongo::*;
use crate::external::mongo_struct::{ContactRow, MediaOwnedRow, NodeFichierRepOwned, NodeFichierVersionOwned};
use crate::flow::directories::{get_path_cuuid, parse_selection_directories};
use crate::flow::transaction_commands::{TransactionAjouterFichiersCollection, TransactionDeplacerFichiersCollection};
use crate::flow::transaction_models::{TransactionDeleteV2, TransactionDeleteV3, TransactionMoveV2, TransactionMoveV2Directory};
use crate::flow::transactions::{delete_file_reps_v3, move_files_collections};
use crate::models::TypeNode;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::hachages::{hacher_bytes, hacher_bytes_vu8};
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageMilleGrillesOwned;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::mongodb::Cursor;
use millegrilles_common_rust::mongodb::options::{DeleteManyModel, InsertOneModel, UpdateManyModel, UpdateOneModel, WriteModel};
use millegrilles_common_rust::multibase::Base;
use millegrilles_common_rust::multihash::Code;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error, warn};
use millegrilles_common_rust::v3::models::{TransactionOperationAggregator, TransactionWrapper};
use millegrilles_common_rust::{bson, hex, serde_json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub async fn legacy_deplacer_fichiers_collection<M>(
    mongo: &M,
    mut wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped {
    debug!("legacy_deplacer_fichiers_collection id:{}", wrapper.message.id);
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(id) => id,
        None => return Err(CommonError::Str("User_id missing from certificate in legacy_deplacer_fichiers_collection transaction"))
    };

    let transaction_collection: TransactionDeplacerFichiersCollection = wrapper.message.deserialize()?;

    // This code is copied and adapted from transaction_commands.move_files_collections()
    let mut session = mongo.get_session().await?;
    session.start_transaction().await?;

    let result = parse_selection_directories(
        mongo,
        user_id.as_str(),
        &transaction_collection.cuuid_destination,
        &transaction_collection.inclure_tuuids,
        true,
        true,
        &mut session
    ).await?;

    // Check that the destination is not under the source (moving under itself breaks the graph)
    if let Some(directory_moves) = result.directories.as_ref() {
        for directory_move in directory_moves {
            for directory in &directory_move.directories {
                if result.destination_path.contains(directory) {
                    return Err(CommonError::Str("commande_deplacer_fichiers_collection Rejecting move directory, the destination is under the source (this breaks the graph)"))
                }
            }
        }
    }

    // Build the transaction, remove certificate
    let mut original_command = wrapper.message.clone();
    original_command.certificat = None;

    let transaction_value = TransactionMoveV2 {
        command: original_command,
        destination: result.destination_path,
        directories: result.directories,
        files: result.files,
        user_id: Some(user_id),
    };

    // Inject the new command in the existing message - keeps id/estampille (note: makes message invalid)
    wrapper.message.contenu = serde_json::to_string(&transaction_value)?;

    debug!("commande_deplacer_fichiers_collection Transaction\n{}", serde_json::to_string(&transaction_value)?);

    // Reuse the new transaction code to produce the operations
    let aggregator = move_files_collections(mongo, wrapper).await?;
    Ok(aggregator)
}


/// The transaction_ajouter_fichiers_collection transaction is deprecated since 2024.9 - replaced by transaction_copy_v2.
/// The command is still used (has been refactored)
pub async fn legacy_ajouter_fichiers_collection<M>(
    mongo: &M,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped {
    debug!("legacy_deplacer_fichiers_collection id:{}", wrapper.message.id);
    let transaction_collection: TransactionAjouterFichiersCollection = wrapper.message.deserialize()?;
    let uuid_transaction = &wrapper.message.id;

    let user_id = match wrapper.certificate.get_user_id()? {
        Some(inner) => inner,
        None => return Err(CommonError::Str("transaction.transaction_ajouter_fichiers_collection Certificat sans user_id"))
    };

    let (user_id_origine, user_id_destination) = match transaction_collection.contact_id.as_ref() {
        Some(contact_id) => {
            debug!("transaction_ajouter_fichiers_collection Verifier que le contact_id est valide (correspond aux tuuids)");
            let collection = mongo.get_collection_typed::<ContactRow>(NOM_COLLECTION_PARTAGE_CONTACT)?;
            let filtre = doc! {CHAMP_CONTACT_ID: contact_id, CHAMP_CONTACT_USER_ID: &user_id};
            let contact = match collection.find_one(filtre).await {
                Ok(inner) => match inner {
                    Some(inner) => inner,
                    None => return Err(CommonError::Str("Invalid contact id"))
                },
                Err(e) => Err(format!("transactions.transaction_ajouter_fichiers_collection Erreur traitement contact_id : {:?}", e))?
            };

            // Retourner le user_id d'origine et destination
            (contact.user_id, Some(user_id))
        },
        None => (user_id, None)  // Origine et destination est le meme user
    };

    debug!("transaction_ajouter_fichiers_collection Copier fichiers user_id {} (vers {:?})", user_id_origine, user_id_destination);

    // Dupliquer la structure de repertoires
    let models = match dupliquer_structure_repertoires(
        mongo,
        uuid_transaction,
        &transaction_collection.cuuid,
        &transaction_collection.inclure_tuuids,
        user_id_origine.as_str(),
        user_id_destination,
    ).await {
        Ok(inner) => inner,
        Err(e) => return Err(CommonError::String(format!("grosfichiers.transaction_ajouter_fichiers_collection Erreur dupliquer_structure_repertoires sur transaction : {:?}", e)))
    };

    let mut aggregator = TransactionOperationAggregator::new();
    // aggregator.legacy = true;  // Forces a commit after
    aggregator.ordered = Some(models);
    Ok(aggregator)
}

#[derive(Clone, Debug)]
struct CopieTuuidVersCuuid {
    tuuid_original: String,
    cuuid_destination: String,
}

/// Duplique la structure des repertoires listes dans tuuids.
/// Les fichiers des sous-repertoires sont linkes (pas copies).
async fn dupliquer_structure_repertoires<M>(
    mongo: &M,
    uuid_transaction: &str,
    cuuid: &str,
    tuuids: &Vec<String>,
    user_id: &str,
    user_id_destination: Option<String>
) -> Result<Vec<WriteModel>, CommonError> where M: MongoDaoTyped {
    let user_id_destination = match user_id_destination.as_ref() {
        Some(inner) => inner.as_ref(),
        None => user_id
    };
    let mut tuuids_remaining: Vec<CopieTuuidVersCuuid> = tuuids.iter().map(|t| {
        CopieTuuidVersCuuid { tuuid_original: t.to_string(), cuuid_destination: cuuid.to_owned() }
    }).collect();

    // let collection = middleware.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let collection_reps =
        mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let collection_versions =
        mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;

    let mut models = Vec::with_capacity(100);

    loop {
        let fichier_rep_tuuid = match tuuids_remaining.pop() {
            Some(inner) => inner,
            None => { break; }  // Termine
        };

        let filtre = doc! {
            CHAMP_TUUID: &fichier_rep_tuuid.tuuid_original,
            CHAMP_USER_ID: user_id,
        };
        let mut curseur = collection_reps.find(filtre).await?;
        if curseur.advance().await? {
            let mut fichier_rep = curseur.deserialize_current()?;
            let type_node = TypeNode::try_from(fichier_rep.type_node.as_str())?;
            let tuuid_src = fichier_rep.tuuid.to_owned();

            debug!("dupliquer_structure_repertoires Copier fichier_rep {} vers {}", fichier_rep_tuuid.tuuid_original, fichier_rep_tuuid.cuuid_destination);

            // Creer nouveau tuuid unique pour le fichier/repertoire a dupliquer
            let nouveau_tuuid_str = format!("{}/{}/{}", uuid_transaction, &fichier_rep_tuuid.cuuid_destination, fichier_rep.tuuid);
            let nouveau_tuuid_multihash = hacher_bytes(nouveau_tuuid_str.into_bytes().as_slice(), Some(Code::Blake2s256), Some(Base::Base16Lower));
            let nouveau_tuuid = (&nouveau_tuuid_multihash[9..]).to_string();
            debug!("dupliquer_structure_repertoires Nouveau tuuid : {:?}", nouveau_tuuid);

            // Remplacer le tuuid
            fichier_rep.tuuid = nouveau_tuuid.clone();

            // Set as not deleted
            fichier_rep.supprime = false;
            fichier_rep.supprime_indirect = false;

            // Recuperer le path du cuuid destination, remplacer path_cuuids
            let path_cuuids_option = match get_path_cuuid(mongo, &fichier_rep_tuuid.cuuid_destination).await {
                Ok(inner) => inner,
                Err(e) => Err(format!("transactions.dupliquer_structure_repertoires get_path_cuuid : {:?}", e))?
            };

            match path_cuuids_option.as_ref() {
                Some(inner) => {
                    fichier_rep.path_cuuids = Some(inner.iter().map(|s| s.to_string()).collect());
                },
                None => {
                    fichier_rep.path_cuuids = None;
                }
            };

            // collection.insert_one_with_session(doc_repertoire, None).await?;
            let filtre = doc! {
                FIELD_TUUID: &fichier_rep.tuuid, FIELD_USER_ID: &user_id_destination,
                // CHAMP_SUPPRIME: false, CHAMP_SUPPRIME_INDIRECT: false,
            };
            let mut set_ops = bson::serialize_to_document(&fichier_rep)?;
            // Ensure the file/directory is not deleted in the destination (allows copying from Trash)
            set_ops.insert(FIELD_DATE_CREATED, Utc::now());
            if user_id_destination != user_id {
                // Changer le user_id (copie de repertoire partage)
                set_ops.insert(FIELD_USER_ID, user_id_destination);
            }

            // Fix set_ops, remove fields updated directly in ops
            set_ops.remove(CHAMP_FLAG_INDEX);
            set_ops.remove(FIELD_DATE_MODIFIED);

            let ops = doc! {
                "$set": {CHAMP_FLAG_INDEX: false},
                "$setOnInsert": set_ops,
                "$currentDate": { FIELD_DATE_MODIFIED: true }
            };
            debug!("dupliquer_structure_repertoires Upsert entry: {:?}", ops);
            models.push(
                WriteModel::UpdateOne(
                    UpdateOneModel::builder()
                        .namespace(collection_reps.namespace())
                        .filter(filtre)
                        .update(ops)
                        .upsert(true)
                        .build()
                )
            );

            match type_node {
                TypeNode::Fichier => {
                    if user_id_destination != user_id {
                        // Copier les versions des fichiers vers le user_id destination
                        let filtre = doc!{CHAMP_TUUIDS: &tuuid_src};
                        let ops = doc!{"$addToSet": {"tuuids": &nouveau_tuuid}, "$currentDate": {FIELD_DATE_MODIFIED: true}};
                        models.push(
                            WriteModel::UpdateMany(
                                UpdateManyModel::builder()
                                    .namespace(collection_versions.namespace())
                                    .filter(filtre)
                                    .update(ops)
                                    .build()
                            )
                        );

                        if let Some(fuuids_versions) = fichier_rep.fuuids_versions.clone() {
                            let operations = copy_media_file(mongo, user_id_destination, user_id, fuuids_versions).await?;
                            models.extend(operations);
                        }
                    }
                },
                TypeNode::Collection | TypeNode::Repertoire => {
                    // Trouver les sous-repertoires, traiter individuellement
                    let filtre_ajout_cuuid = doc! {
                        format!("{}.0", CHAMP_PATH_CUUIDS): &tuuid_src,
                        "$or": [
                            {CHAMP_SUPPRIME: false},
                            {CHAMP_SUPPRIME_INDIRECT: true},  // Implies indirectly deleted. Used to copy out of Trash.
                        ],
                    };
                    debug!("dupliquer_structure_repertoires filtre_ajout_cuuid : {:?}", filtre_ajout_cuuid);
                    let mut curseur = collection_reps.find(filtre_ajout_cuuid).await?;
                    while curseur.advance().await? {
                        let sous_document = curseur.deserialize_current()?;
                        let copie_tuuid = CopieTuuidVersCuuid {
                            tuuid_original: sous_document.tuuid.to_owned(),
                            cuuid_destination: nouveau_tuuid.clone()
                        };
                        debug!("dupliquer_structure_repertoires Parcourir sous-fichier/rep {:?}", copie_tuuid);
                        tuuids_remaining.push(copie_tuuid);
                    }
                }
            }
        }
    }

    Ok(models)
}

async fn copy_media_file<M>(
    mongo: &M,
    user_id: &str,
    original_user_id: &str,
    fuuids_versions: Vec<String>
) -> Result<Vec<WriteModel>, CommonError> where M: MongoDaoTyped {
    debug!("copy_media_file fuuids_versions: {:?}, user_id: {:?}", fuuids_versions, user_id);
    let collection_media = mongo.get_collection_typed::<MediaOwnedRow>(NOM_COLLECTION_MEDIA)?;

    let mut models = Vec::new();

    let filtre_media = doc! {"fuuid": {"$in": &fuuids_versions}, "user_id": original_user_id};
    let mut cursor = collection_media.find(filtre_media).await?;
    while cursor.advance().await? {
        let row = cursor.deserialize_current()?;
        let filtre = doc! {"fuuid": &row.fuuid, "user_id": user_id};

        let mut set_on_insert = doc! {
            FIELD_DATE_CREATED: Utc::now(),
            // "mimetype": row.mimetype,
            "height": row.height,
            "width": row.width,
            "duration": row.duration,
            "videoCodec": row.video_codec,
            "anime": row.anime,
        };

        if let Some(images) = row.images {
            set_on_insert.insert("images", bson::serialize_to_document(&images)?);
        }
        if let Some(audio) = row.audio {
            set_on_insert.insert("audio", bson::serialize_to_bson(&audio)?);
        }
        if let Some(subtitles) = row.subtitles {
            set_on_insert.insert("subtitles", bson::serialize_to_bson(&subtitles)?);
        }
        if let Some(web_subtitles) = row.web_subtitles {
            set_on_insert.insert("web_subtitles", bson::serialize_to_bson(&web_subtitles)?);
        }
        let mut ops = doc! {
            "$setOnInsert": set_on_insert,
            "$currentDate": { FIELD_DATE_MODIFIED: true }
        };
        // Copy videos individually to merge the entries.
        if let Some(video) = row.video {
            let mut set_ops = doc! {};
            for (key, value) in video {
                set_ops.insert(format!("video.{}", key), bson::serialize_to_document(&value)?);
            }
            if !set_ops.is_empty() {
                ops.insert("$set", set_ops);
            }
        }
        debug!("copy_media_file ops {:?}", ops);
        models.push(
            WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(collection_media.namespace())
                    .filter(filtre)
                    .update(ops)
                    .upsert(true)
                    .build()
            )
        );
    }
    Ok(models)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TransactionSupprimerDocuments {
    tuuids: Vec<String>,    // Fichiers/rep a supprimer
    cuuid: Option<String>,  // Collection a retirer des documents (suppression conditionnelle)
}

pub async fn legacy_supprimer_documents<M>(
    mongo: &M,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped {
    debug!("legacy_supprimer_documents id:{}", wrapper.message.id);
    let command: TransactionSupprimerDocuments = wrapper.message.deserialize()?;
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(inner) => inner,
        None => return Err(CommonError::Str("transaction.transaction_ajouter_fichiers_collection Certificat sans user_id"))
    };

    // Cumulate multiple calls to transaction delete
    let mut aggregator = TransactionOperationAggregator::new();

    // Separate the directories from the files.
    let collection_fichierrep = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre = doc!{
        FIELD_TUUID: { "$in": &command.tuuids }
    };
    let mut cursor = collection_fichierrep.find(filtre).await?;
    let mut files = Vec::new();
    let mut directories = Vec::new();
    let mut tuuids_by_cuuid: HashMap<String, Vec<String>> = HashMap::new();
    while let Some(row) = cursor.next().await {
        let row = row?;
        if row.user_id != user_id.as_str() {
            warn!("commande_supprimer_documents Deleting file with wrong user_id, SKIPPING");
            continue
        }
        if row.type_node == TypeNode::Fichier.to_str() {
            files.push(row.tuuid.to_owned());
        } else {
            directories.push(row.tuuid.to_owned());
        }

        // Map all tuuids for post-transaction cleanup (search index, events)
        let cuuid = match row.path_cuuids.as_ref() {
            Some(path_cuuids) => match path_cuuids.get(0) {
                Some(cuuid) => cuuid.as_str(),
                None => user_id.as_str(),
            },
            None => user_id.as_str()
        };
        match tuuids_by_cuuid.get_mut(cuuid) {
            Some(tuuids) => tuuids.push(row.tuuid.to_owned()),
            None => {
                tuuids_by_cuuid.insert(cuuid.to_owned(), vec![row.tuuid.to_owned()]);
            }
        }
    }

    // Make a list of all subdirectories to delete.
    // Only include subdirectories that are not already deleted.
    let mut subdirectories = Vec::new();
    let filtre = doc!{
        FIELD_PATH_CUUIDS: { "$in": &directories },
        FIELD_NODE_TYPE: TypeNode::Repertoire.to_str(),
        FIELD_DELETED: false
    };
    let mut cursor = collection_fichierrep.find(filtre).await?;
    while let Some(row) = cursor.next().await {
        let row = row?;
        if row.user_id != user_id.as_str() {
            warn!("commande_supprimer_documents Deleting directory with wrong user_id, SKIPPING");
            continue
        }
        subdirectories.push(row.tuuid.to_owned());

        // Add all directory tuuids for post-transaction cleanup (search index, events)
        let cuuid = match row.path_cuuids.as_ref() {
            Some(path_cuuids) => match path_cuuids.get(0) {
                Some(cuuid) => cuuid.as_str(),
                None => user_id.as_str(),
            },
            None => user_id.as_str()
        };
        match tuuids_by_cuuid.get_mut(cuuid) {
            Some(tuuids) => tuuids.push(row.tuuid.to_owned()),
            None => {
                tuuids_by_cuuid.insert(cuuid.to_owned(), vec![row.tuuid.to_owned()]);
            }
        }
    }

    // Make a list of all files that will be affected.
    // Those will be sent in (potentially) batched transactions up-front. This removes the
    // files explicitly from the versions collection.
    // This is also for post-transaction cleanup (search index, events).
    let mut subfiles = Vec::with_capacity(5_000);
    let filtre = doc!{
        FIELD_PATH_CUUIDS: { "$in": &directories },
        FIELD_NODE_TYPE: TypeNode::Fichier.to_str(),
        FIELD_DELETED: false
    };
    let mut cursor = collection_fichierrep.find(filtre).await?;
    while let Some(row) = cursor.next().await {
        let row = row?;
        if row.user_id != user_id.as_str() {
            warn!("commande_supprimer_documents Deleting directory with wrong user_id, SKIPPING");
            continue
        }
        subfiles.push(row.tuuid.to_owned());

        if subfiles.len() == 5_000 {
            // Submit as a batch
            let transaction_value = TransactionDeleteV3 {
                command: None,
                directories: None,
                subdirectories: None,
                subfiles: Some(subfiles),
                files: None,
                user_id: Some(user_id.clone()),
            };
            subfiles = Vec::with_capacity(5_000);
            // Reuse the existing wrapper
            let mut wrapper_clone = wrapper.clone();
            wrapper_clone.message.contenu = serde_json::to_string(&transaction_value)?;
            let ops = delete_file_reps_v3(mongo, wrapper_clone).await?;
            aggregator.merge(ops)?;
        }

        let cuuid = match row.path_cuuids.as_ref() {
            Some(path_cuuids) => match path_cuuids.get(0) {
                Some(cuuid) => cuuid.as_str(),
                None => user_id.as_str(),
            },
            None => user_id.as_str()
        };
        match tuuids_by_cuuid.get_mut(cuuid) {
            Some(tuuids) => tuuids.push(row.tuuid.to_owned()),
            None => {
                tuuids_by_cuuid.insert(cuuid.to_owned(), vec![row.tuuid.to_owned()]);
            }
        }
    }

    // Inject message in new command but remove certificate
    let mut message_clone = wrapper.message.clone();
    message_clone.certificat = None;

    // Create a new transaction with information to process
    let transaction_value = TransactionDeleteV3 {
        command: Some(message_clone),
        directories: match directories.len() {
            0 => None,
            _ => Some(directories)
        },
        subdirectories: match subdirectories.len() {
            0 => None,
            _ => Some(subdirectories)
        },
        subfiles: match subfiles.len() {
            0 => None,
            _ => Some(subfiles)
        },
        files: match files.len() {
            0 => None,
            _ => Some(files)
        },
        user_id: Some(user_id.clone()),
    };

    // Reuse the existing wrapper
    let mut wrapper_clone = wrapper.clone();
    wrapper_clone.message.contenu = serde_json::to_string(&transaction_value)?;
    let ops = delete_file_reps_v3(mongo, wrapper_clone).await?;
    aggregator.merge(ops)?;

    Ok(aggregator)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionListeDocuments {
    pub tuuids: Vec<String>,  // Fichiers/rep a supprimer, archiver, etc
}

pub async fn legacy_recuperer_documents<M>(
    mongo: &M,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped
{
    debug!("legacy_recuperer_documents id:{}", wrapper.message.id);
    let transaction_collection: TransactionListeDocuments = wrapper.message.deserialize()?;

    // Conserver champs transaction uniquement (filtrer champs meta)
    let filtre = doc! { FIELD_TUUID: { "$in": &transaction_collection.tuuids } };
    let ops = doc! {
        "$set": { FIELD_DELETED: false },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };

    let mut models = Vec::new();

    let collection_reps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    models.push(
        WriteModel::UpdateMany(
            UpdateManyModel::builder()
                .namespace(collection_reps.namespace())
                .filter(filtre)
                .update(ops)
                .build()
        )
    );

    // Put the tuuid back in versions
    let collection_versions = mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;
    let filtre = doc! { FIELD_TUUID: { "$in": &transaction_collection.tuuids }, FIELD_NODE_TYPE: TypeNode::Fichier.to_str() };
    let mut cursor = collection_reps.find(filtre.clone()).await?;
    while let Some(row) = cursor.next().await {
        let row = row?;
        if let Some(fuuids) = row.fuuids_versions {
            let filtre_versions = doc! { FIELD_FUUID: { "$in": fuuids } };
            let ops = doc! { "$addToSet": { FIELD_TUUIDS: row.tuuid }, "$currentDate": { FIELD_DATE_MODIFIED: true } };
            models.push(
                WriteModel::UpdateMany(
                    UpdateManyModel::builder()
                        .namespace(collection_versions.namespace())
                        .filter(filtre_versions)
                        .update(ops)
                        .build()
                )
            );
        }
    }

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);
    Ok(aggregator)
}

#[derive(Deserialize)]
pub struct TransactionSupprimerVideo {
    pub fuuid_video: String,
}

pub async fn legacy_supprimer_video<M>(
    mongo: &M,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped {
    debug!("legacy_supprimer_video id:{}", wrapper.message.id);
    let transaction_value: TransactionSupprimerVideo = wrapper.message.deserialize()?;

    let fuuid = &transaction_value.fuuid_video;

    let user_id = match wrapper.certificate.get_user_id()? {
        Some(id) => id,
        None => return Err(CommonError::Str("User_id missing from certificate in legacy_supprimer_video transaction"))
    };

    // Find the original fuuid for the video
    let filtre_versions = doc!{"fuuids_reclames": fuuid};
    let collection_versions =
        mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;
    let file_version = match collection_versions.find_one(filtre_versions.clone()).await? {
        Some(inner) => inner,
        None => Err(format!("transaction_supprimer_video No matching file version found with fuuid {}", fuuid))?
    };
    let fuuid_original = file_version.fuuid.as_str();

    // Load media information for original fuuid/user_id
    let filtre_media = doc!{ FIELD_FUUID: fuuid_original, FIELD_USER_ID: &user_id };
    let collection_media = mongo.get_collection_typed::<MediaOwnedRow>(NOM_COLLECTION_MEDIA)?;
    let doc_video = match collection_media.find_one(filtre_media.clone()).await {
        Ok(d) => match d {
            Some(d) => d,
            None => Err(format!("transaction_supprimer_video No document match on user_id:{}/fuuid:{}", user_id, fuuid))?
        },
        Err(e) => Err(format!("transaction_supprimer_video Erreur chargement info document : {:?}", e))?
    };

    // Find video label matching video fuuid to remove
    let mut ops_unset_video = doc!{};
    if let Some(map_video) = doc_video.video.as_ref() {
        for (label, video) in map_video {
            if &video.fuuid_video == fuuid {
                ops_unset_video.insert(format!("video.{}", label), true);
            }
        }
    }

    // Ensure there is a video with that fuuid
    if ops_unset_video.len() == 0 {
        return Ok(TransactionOperationAggregator::new())  // Nothing to do
    }

    let mut models = Vec::new();

    // Remove video with matching fuuid
    let ops_media = doc!{"$unset": ops_unset_video, "$currentDate": { FIELD_DATE_MODIFIED: true }};
    models.push(
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection_media.namespace())
                .filter(filtre_media)
                .update(ops_media)
                .build()
        )
    );

    // Cleanup file version (claimed fuuids)
    {
        let ops = doc! {
            "$pull": {CHAMP_FUUIDS_RECLAMES: fuuid},
            "$currentDate": { FIELD_DATE_MODIFIED: true },
        };
        let collection_version_fichier = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;

        debug!("transaction_supprimer_video Supprimer video {:?} ops {:?}", filtre_versions, ops);
        models.push(
            WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(collection_version_fichier.namespace())
                    .filter(filtre_versions)
                    .update(ops)
                    .build()
            )
        );
    }

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);
    Ok(aggregator)
}

#[derive(Deserialize)]
pub struct TransactionRecupererDocumentsV2 {
    /// Items a recuperer par { cuuid: [tuuid, ...] }
    pub items: HashMap<String, Option<Vec<String>>>
}

pub async fn legacy_recuperer_documents_v2<M>(
    mongo: &M,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped
{
    debug!("legacy_deplacer_fichiers_collection id:{}", wrapper.message.id);
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(id) => id,
        None => return Err(CommonError::Str("User_id missing from certificate in legacy_recuperer_documents_v2 transaction"))
    };

    let transaction_value: TransactionRecupererDocumentsV2 = wrapper.message.deserialize()?;

    let mut models: Vec<WriteModel> = Vec::with_capacity(100);

    for (cuuid, paths) in transaction_value.items {
        // Recuperer le cuuid (parent) jusqu'a la racine au besoin
        debug!("transaction_recuperer_documents_v2 Recuperer cuuid {} et parents", cuuid);
        match recuperer_parents(mongo, &user_id, &cuuid).await {
            Ok(inner) => models.extend(inner),
            Err(e) => return Err(CommonError::String(format!("grosfichiers.transaction_recuperer_documents_v2 Erreur recuperer_parents : {:?}", e)))
        }

        // Reactiver les fichiers avec le cuuid courant sous cuuids_supprimes.
        debug!("transaction_recuperer_documents_v2 Recuperer tuuids {:?} sous cuuid {}", paths, cuuid);
        match recuperer_tuuids(mongo, &user_id, cuuid.as_str(), paths).await {
            Ok(inner) => models.extend(inner),
            Err(e) => return Err(CommonError::String(format!("grosfichiers.transaction_recuperer_documents_v2 Erreur recuperer_tuuids : {:?}", e)))
        }
    }

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);
    Ok(aggregator)
}

async fn recuperer_parents<M>(mongo: &M, user_id: &str, tuuid: &str) -> Result<Vec<WriteModel>, CommonError>
where M: MongoDaoTyped
{
    let filtre = doc!{ CHAMP_TUUID: tuuid, CHAMP_USER_ID: user_id };
    let collection = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let repertoire = match collection.find_one(filtre).await? {
        Some(inner) => inner,
        None => Err(format!("recuperer_parents Tuuid inconnu {}", tuuid))?
    };

    // let type_node = match repertoire.type_node {
    //     Some(inner) => TypeNode::try_from(inner.as_str())?,
    //     None => Err(format!("recuperer_parents Node sans information de type {}", tuuid))?
    // };
    let type_node = TypeNode::try_from(repertoire.type_node.as_str())?;

    let cuuids = match type_node {
        TypeNode::Fichier => Err(format!("recuperer_parents Node {} de type Fichier, invalide pour reactiver parents", tuuid))?,
        TypeNode::Collection => {
            // Ok, aucuns parents a reactiver
            vec![tuuid.to_owned()]
        },
        TypeNode::Repertoire => {
            match repertoire.path_cuuids {
                Some(mut inner) => {
                    inner.push(tuuid.to_owned());  // Ajouter le repertoire lui-meme
                    inner
                },
                None => Err(format!("recuperer_parents Node {} est un Repertoire sans path_cuuids, invalide pour reactiver parents", tuuid))?,
            }
        }
    };

    let filtre = doc! { CHAMP_TUUID: {"$in": cuuids} };
    let ops = doc! {
        "$set": {
            CHAMP_SUPPRIME: false,
            CHAMP_SUPPRIME_INDIRECT: false,
        },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let mut models = Vec::new();

    models.push(
        WriteModel::UpdateMany(
            UpdateManyModel::builder()
                .namespace(collection.namespace())
                .filter(filtre)
                .update(ops)
                .build()
        )
    );

    Ok(models)
}


async fn recuperer_tuuids<M>(
    mongo: &M,
    user_id: &str,
    cuuid: &str,
    tuuids_params: Option<Vec<String>>,
) -> Result<Vec<WriteModel>, CommonError> where M: MongoDaoTyped {
    let tuuids: Vec<String> = match tuuids_params {
        Some(inner) => inner,
        None => vec![cuuid.to_owned()]  // Recuperer une collection
    };

    debug!("recuperer_tuuids Recuperer tuuids {:?}", tuuids);
    let mut nodes_restants = Vec::new();

    // Charger liste initiale de nodes
    let collection_reps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    {
        let filtre = doc! {
            CHAMP_TUUID: { "$in": tuuids },
            CHAMP_USER_ID: user_id
        };
        let mut curseur = collection_reps.find(filtre).await?;
        while curseur.advance().await? {
            let row = curseur.deserialize_current()?;
            nodes_restants.push(row);
        }
    }

    let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;

    // Parcourir les nodes. Recuperer recursivement les Repertoires avec supprime_indirect: true.
    let mut models = Vec::new();
    let mut tuuids_a_recuperer = Vec::new();
    // let mut fuuids_a_recuperer = Vec::new();
    loop {
        let node_courant = match nodes_restants.pop() {
            Some(inner) => inner,
            None => { break; }  // Termine
        };

        let type_node = TypeNode::try_from(node_courant.type_node.as_str())?;

        match type_node {
            TypeNode::Fichier => {
                if let Some(fuuids) = node_courant.fuuids_versions {
                    // fuuids_a_recuperer.extend(fuuids);
                    debug!("recuperer_tuuids Recuperer {} fuuids", fuuids.len());
                    let filtre = doc! {CHAMP_FUUID: {"$in": fuuids}};
                    let ops = doc! {
                        // "$set": { CHAMP_SUPPRIME: false },
                        "$addToSet": {"tuuids": &node_courant.tuuid},
                        "$currentDate": { FIELD_DATE_MODIFIED: true }
                    };
                    models.push(
                        WriteModel::UpdateMany(
                            UpdateManyModel::builder()
                                .namespace(collection_versions.namespace())
                                .filter(filtre)
                                .update(ops)
                                .build()
                        )
                    );
                }
            },
            TypeNode::Collection | TypeNode::Repertoire => {
                // Charger la liste des nodes supprimes indirectement sous ce repertoire
                let filtre = doc! {
                    CHAMP_USER_ID: user_id,
                    "path_cuuids.0": &node_courant.tuuid,
                    CHAMP_SUPPRIME_INDIRECT: true
                };
                let mut curseur = collection_reps.find(filtre).await?;
                while curseur.advance().await? {
                    let row = curseur.deserialize_current()?;
                    nodes_restants.push(row);
                }
            }
        }

        tuuids_a_recuperer.push(node_courant.tuuid);
    }

    {
        debug!("recuperer_tuuids Recuperer {} tuuids", tuuids_a_recuperer.len());
        let filtre = doc! { CHAMP_USER_ID: user_id, CHAMP_TUUID: {"$in": tuuids_a_recuperer}};
        let ops = doc! {
            "$set": {CHAMP_SUPPRIME: false, CHAMP_SUPPRIME_INDIRECT: false },
            "$currentDate": { FIELD_DATE_MODIFIED: true }
        };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection_reps.namespace())
                    .filter(filtre)
                    .update(ops)
                    .build()
            )
        );
    }

    Ok(models)
}

#[derive(Deserialize)]
pub struct TransactionSupprimerOrphelins {
    pub fuuids: Vec<String>,
}

pub async fn legacy_supprimer_orphelins<M>(
    mongo: &M,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped {
    debug!("legacy_deplacer_fichiers_collection id:{}", wrapper.message.id);

    let transaction_value: TransactionSupprimerOrphelins = wrapper.message.deserialize()?;

    let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let collection_media = mongo.get_collection(NOM_COLLECTION_MEDIA)?;

    let mut models = Vec::with_capacity(transaction_value.fuuids.len() * 4);

    // Delete all matching media
    let filtre_versions = doc!{ FIELD_FUUID: {"$in": &transaction_value.fuuids} };
    models.push(
        WriteModel::DeleteMany(
            DeleteManyModel::builder()
                .namespace(collection_media.namespace())
                .filter(filtre_versions)
                .build()
        )
    );

    // Delete all matching versions
    let filtre_versions = doc!{ FIELD_FUUID: {"$in": &transaction_value.fuuids} };
    models.push(
        WriteModel::DeleteMany(
            DeleteManyModel::builder()
                .namespace(collection_versions.namespace())
                .filter(filtre_versions)
                .build()
        )
    );

    // Remove versions from all filereps
    let filtre_reps = doc!{ FIELD_FUUIDS_VERSIONS: {"$in": &transaction_value.fuuids} };
    let ops_reps = doc! {
        "$pullAll": { FIELD_FUUIDS_VERSIONS: &transaction_value.fuuids },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    models.push(
        WriteModel::UpdateMany(
            UpdateManyModel::builder()
                .namespace(collection_reps.namespace())
                .filter(filtre_reps)
                .update(ops_reps)
                .build()
        )
    );

    // Final cleanup for all filereps that are deleted and with no fileversions left.
    let filtre_delete_reps = doc!{
        FIELD_DELETED: true,
        FIELD_FUUIDS_VERSION_0: {"exists": false},
        FIELD_NODE_TYPE: TypeNode::Fichier.to_str()
    };
    models.push(
        WriteModel::DeleteMany(
            DeleteManyModel::builder()
                .namespace(collection_reps.namespace())
                .filter(filtre_delete_reps)
                .build()
        )
    );

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);
    Ok(aggregator)
}

#[derive(Serialize, Deserialize)]
pub struct TransactionCopyV2 {
    pub command: MessageMilleGrillesOwned,
    pub destination: Vec<String>,
    pub directories: Option<Vec<TransactionMoveV2Directory>>,
    pub files: Option<Vec<String>>,
    pub user_id: String,
    pub include_deleted: Option<bool>,
}

fn digest_new_tuuid(transaction_id_bytes: &Vec<u8>, old_tuuid: &str) -> String {
    let mut bytes_to_hash = old_tuuid.as_bytes().to_vec();
    bytes_to_hash.extend(transaction_id_bytes);
    let digest_value = hacher_bytes_vu8(&bytes_to_hash[..], Some(Code::Blake2s256));
    hex::encode(digest_value)
}

pub async fn legacy_copy_v2<M>(
    middleware: &M,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped
{
    debug!("legacy_deplacer_fichiers_collection id:{}", wrapper.message.id);
    let transaction_value: TransactionCopyV2 = wrapper.message.deserialize()?;

    let user_id = match wrapper.certificate.get_user_id()? {
        Some(id) => id,
        None => transaction_value.user_id.clone(),
    };

    let created = wrapper.message.estampille.clone();

    let include_deleted = transaction_value.include_deleted.unwrap_or(false);

    let collection_reps =
        middleware.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;

    let transaction_id = wrapper.message.id.clone();
    let transaction_id_bytes = hex::decode(&transaction_id)?;

    let mut models = Vec::new();

    if let Some(files) = transaction_value.files {
        let destination = &transaction_value.destination;
        let filtre = doc!{"tuuid": {"$in": &files}};
        let cursor_reps = collection_reps.find(filtre.clone()).await?;
        match copy_files_to_directory(middleware, user_id.as_str(), &created, cursor_reps, destination, &transaction_id_bytes).await {
            Ok(inner) => models.extend(inner),
            Err(err) => return Err(err)
        }
    }

    if let Some(directories) = transaction_value.directories {
        // Prepare a mapping for all the new directory tuuids - will be used to adjust the new path_cuuids.
        let mut new_cuuid_map = HashMap::new();
        for copy_directories in &directories {
            for directory in &copy_directories.directories {
                let new_directory_tuuid = digest_new_tuuid(&transaction_id_bytes, directory.as_str());
                new_cuuid_map.insert(directory, new_directory_tuuid);
            }
        }

        // Copy files and directories
        for copy_directories in &directories {
            for directory in &copy_directories.directories {
                let mut destination: Vec<String> = copy_directories.path.iter()
                    .map(|s| {
                        match new_cuuid_map.get(s) {
                            Some(inner) => inner.to_string(),  // New cuuid
                            None => s.to_string()  // Keep old cuuid (part of destination parent hierarchy)
                        }
                    })
                    .collect();
                // Copy the directory entry
                let filtre = doc!{"tuuid": &directory};
                let mut cursor_reps = collection_reps.find(filtre.clone()).await?;
                let new_tuuid = new_cuuid_map.get(&directory).expect("get new tuuid");
                if cursor_reps.advance().await? {
                    let mut row = cursor_reps.deserialize_current()?;
                    row.tuuid = new_tuuid.clone();
                    row.user_id = user_id.clone();  // In case this is a copy from shared collections
                    row.path_cuuids = Some(destination.clone());
                    row.type_node = TypeNode::Repertoire.to_str().to_string();  // It is possible to copy a Collection to another one
                    row.flag_index = false;  // Need to index new path
                    row.created = created.clone();
                    row.modified = Utc::now();

                    // Remove deleted flags if present. Allows copying out of Trash.
                    row.supprime = false;
                    row.supprime_indirect = false;

                    // Insert the directory with the updated identifiers
                    debug!("transaction_copy_v2 Copy (directories) tuuid {} to {}", directory, row.tuuid);
                    models.push(
                        WriteModel::InsertOne(
                            InsertOneModel::builder()
                                .namespace(collection_reps.namespace())
                                .document(bson::serialize_to_document(&row)?)
                                .build()
                        )
                    );
                } else {
                    error!("transaction_copy_v2 Directory {} not found during copy, skipping", directory);
                    continue
                }

                // Copy all files
                destination.insert(0, new_tuuid.clone());  // Prepend parent directory for files
                let mut filtre = doc!{
                    FIELD_PATH_CUUIDS_0: &directory,
                    FIELD_NODE_TYPE: TypeNode::Fichier.to_str(),
                };
                if include_deleted {
                    // Include files that have been indirectly deleted (deleted with the directory)
                    filtre.insert("$or", vec![
                        doc!{CHAMP_SUPPRIME: false},
                        doc!{CHAMP_SUPPRIME_INDIRECT: true},
                    ]);
                } else {
                    // Exclude all deleted files
                    filtre.insert("supprime", false);
                }
                let cursor_reps = collection_reps.find(filtre.clone()).await?;
                copy_files_to_directory(middleware, user_id.as_str(), &created, cursor_reps, &destination, &transaction_id_bytes).await?;
            }
        }
    }

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);
    Ok(aggregator)
}

async fn copy_files_to_directory<M>(
    middleware: &M, user_id: &str, created: &DateTime<Utc>, mut cursor_reps: Cursor<NodeFichierRepOwned>,
    destination: &Vec<String>, transaction_id_bytes: &Vec<u8>) -> Result<Vec<WriteModel>, CommonError>
where M: MongoDaoTyped
{
    let collection_reps =
        middleware.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let collection_versions =
        middleware.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;

    let now = Utc::now();

    let mut models = Vec::new();

    while cursor_reps.advance().await? {
        let mut row = cursor_reps.deserialize_current()?;
        let original_user_id = row.user_id.clone();
        let change_user = original_user_id.as_str() != user_id;

        let old_tuuid = row.tuuid.clone();

        // Update path_cuuids and identifier. Then re-insert.
        // Hash the old tuuid and the current transaction id with blake2s to get a new unique tuuid.
        let new_tuuid = digest_new_tuuid(transaction_id_bytes, row.tuuid.as_str());
        row.tuuid = new_tuuid.clone();
        row.user_id = user_id.to_string();  // We may be copying from shared collections
        row.path_cuuids = Some(destination.to_owned());
        row.flag_index = false;  // Need to index new path
        row.created = created.clone();
        row.modified = now.clone();

        // Remove deleted flags. Allows copying out of the Trash area.
        row.supprime = false;
        row.supprime_indirect = false;

        let fuuids_versions = row.fuuids_versions.clone();

        // Insert the row with the updated identifiers
        debug!("transaction_copy_v2 Copy (files-1) tuuid {} to {}", old_tuuid, row.tuuid);
        models.push(
            WriteModel::InsertOne(
                InsertOneModel::builder()
                    .namespace(collection_reps.namespace())
                    .document(bson::serialize_to_document(&row)?)
                    .build()
            )
        );

        // Add tuuid to versions
        if let Some(fuuids_versions) = fuuids_versions {
            let filtre_versions = doc! {"fuuid": {"$in": &fuuids_versions}};
            let ops = doc! { "$addToSet": {"tuuids": &new_tuuid}, "$currentDate": { FIELD_DATE_MODIFIED: true } };
            debug!("transaction_copy_v2 Filtre {:?}, ops: {:?}", filtre_versions, ops);
            models.push(
                WriteModel::UpdateMany(
                    UpdateManyModel::builder()
                        .namespace(collection_versions.namespace())
                        .filter(filtre_versions)
                        .update(ops)
                        .build()
                )
            );

            if change_user {
                match copy_media_file(middleware, user_id, original_user_id.as_str(), fuuids_versions).await {
                    Ok(inner) => models.extend(inner),
                    Err(e) => return Err(CommonError::String(format!("Error in copy_media_file: {:?}", e)))
                }
            }
        }
    }

    Ok(models)
}

pub async fn legacy_delete_v2<M>(
    middleware: &M,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped
{
    debug!("legacy_deplacer_fichiers_collection id:{}", wrapper.message.id);
    let transaction_value: TransactionDeleteV2 = wrapper.message.deserialize()?;

    let collection_fichiersrep =
        middleware.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let collection_fichiersversion =
        middleware.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;

    let mut models = Vec::with_capacity(100);

    // Handle subdirectories first
    if let Some(subdirectories) = transaction_value.subdirectories {
        debug!("transaction_delete_v2 Delete subdirectories: {:?}", subdirectories);
        let operations = delete_file_versions_from_directories(middleware, &subdirectories).await?;
        models.extend(operations);

        // Mark all the files as indirectly deleted in the rep collection
        let filtre_rep = doc! {
            FIELD_PATH_CUUIDS_0: {"$in": &subdirectories},
            FIELD_DELETED: false,
            FIELD_NODE_TYPE: TypeNode::Fichier.to_str()
        };
        let ops = doc!{
            "$set": { FIELD_DELETED: true, FIELD_DELETED_INDIRECT: true },
            "$currentDate": { FIELD_DATE_MODIFIED: true }
        };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection_fichiersrep.namespace())
                    .filter(filtre_rep)
                    .update(ops)
                    .build()
            )
        );

        // Mark all listed subdirectories as deleted indirectly.
        let directory_filtre = doc!{ FIELD_TUUID: {"$in": subdirectories} };
        let directory_ops = doc!{
            "$set": { FIELD_DELETED: true, FIELD_DELETED_INDIRECT: true, CHAMP_FLAG_INDEX: false},
            "$currentDate": { FIELD_DATE_MODIFIED: true }
        };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection_fichiersrep.namespace())
                    .filter(directory_filtre)
                    .update(directory_ops)
                    .build()
            )
        );

    }

    // Directories being directly targeted by the delete command
    if let Some(directories) = transaction_value.directories {
        debug!("transaction_delete_v2 Delete directories: {:?}", directories);
        let operations = delete_file_versions_from_directories(middleware, &directories).await?;
        models.extend(operations);

        // Mark all the files as indirectly deleted in the rep collection
        let filtre_rep = doc! {
            FIELD_PATH_CUUIDS_0: { "$in": &directories },
            FIELD_DELETED: false,
            FIELD_NODE_TYPE: TypeNode::Fichier.to_str()
        };
        let ops = doc!{
            "$set": { FIELD_DELETED: true, FIELD_DELETED_INDIRECT: true, CHAMP_FLAG_INDEX: false },
            "$currentDate": { FIELD_DATE_MODIFIED: true }
        };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection_fichiersrep.namespace())
                    .filter(filtre_rep)
                    .update(ops)
                    .build()
            )
        );

        // Mark all listed directories as deleted directly.
        let directory_filtre = doc!{ FIELD_TUUID: { "$in": directories } };
        let directory_ops = doc!{
            "$set": { FIELD_DELETED: true, FIELD_DELETED_INDIRECT: false, CHAMP_FLAG_INDEX: false },
            "$currentDate": { FIELD_DATE_MODIFIED: true }
        };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection_fichiersrep.namespace())
                    .filter(directory_filtre)
                    .update(directory_ops)
                    .build()
            )
        );

    }

    if let Some(files) = transaction_value.files {
        debug!("transaction_delete_v2 Delete files: {:?}", files);
        // Mark all listed files as deleted.

        let version_filtre = doc!{ FIELD_TUUIDS: { "$in": &files } };
        let version_ops = doc!{
            "$pullAll": { FIELD_TUUIDS: &files },
            "$currentDate": { FIELD_DATE_MODIFIED: true }
        };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection_fichiersversion.namespace())
                    .filter(version_filtre)
                    .update(version_ops)
                    .build()
            )
        );

        let file_filtre = doc!{ FIELD_TUUID: {"$in": &files} };
        let rep_ops = doc!{
            "$set": { FIELD_DELETED: true, FIELD_DELETED_INDIRECT: false, CHAMP_FLAG_INDEX: false},
            "$currentDate": {FIELD_DATE_MODIFIED: true}
        };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection_fichiersrep.namespace())
                    .filter(file_filtre)
                    .update(rep_ops)
                    .build()
            )
        );
    }

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);
    Ok(aggregator)
}

async fn delete_file_versions_from_directories<M>(
    middleware: &M,
    subdirectories: &Vec<String>
) -> Result<Vec<WriteModel>, CommonError> where M: MongoDaoTyped {
    // Iterate through all directories in fichiers rep to find matching files. The recursive
    // work of finding subdirectories has already been done in the command.

    let collection_fichiersrep =
        middleware.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let collection_fichiersversion =
        middleware.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;

    let mut models = Vec::new();

    for cuuid in subdirectories {
        // List all files directly under the cuuid (and not already deleted)
        let filtre_files = doc! {
            FIELD_PATH_CUUIDS_0: cuuid,
            FIELD_DELETED: false,
            FIELD_NODE_TYPE: TypeNode::Fichier.to_str()
        };
        let mut cursor = collection_fichiersrep.find(filtre_files.clone()).await?;
        let mut file_tuuids = Vec::new();
        while cursor.advance().await? {
            let row = cursor.deserialize_current()?;
            file_tuuids.push(row.tuuid.to_owned());
        }

        // Mark all files in the version collection as deleted
        let filtre_tuuids = doc! { FIELD_TUUIDS: { "$in": &file_tuuids } };
        let version_ops = doc!{
            "$pullAll": { FIELD_TUUIDS: file_tuuids },
            "$currentDate": { FIELD_DATE_MODIFIED: true }
        };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection_fichiersversion.namespace())
                    .filter(filtre_tuuids)
                    .update(version_ops)
                    .build()
            )
        );
    }

    Ok(models)
}

#[derive(Deserialize)]
pub struct TransactionRecycleItemsV3 {
    /// Original command
    // pub command: MessageMilleGrillesOwned,

    /// User Id
    pub user_id: String,

    /// Files to restore.
    pub file_tuuids: Vec<String>,

    /// Directories to restore.
    /// All files with supprime_indirect == true in those directories will be restored.
    pub directory_tuuids: Vec<String>
}

pub async fn legacy_recycle_items_v3<M>(
    mongo: &M,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped
{
    debug!("legacy_deplacer_fichiers_collection id:{}", wrapper.message.id);
    let transaction_value: TransactionRecycleItemsV3 = wrapper.message.deserialize()?;

    let user_id = match wrapper.certificate.get_user_id()? {
        Some(id) => id,
        None => transaction_value.user_id.clone(),  // return Err(CommonError::Str("User_id missing from certificate in legacy_recycle_items_v3 transaction"))
    };

    let collection = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;

    let mut models = Vec::new();

    if transaction_value.file_tuuids.len() > 0 {
        let operations = recycle_files(mongo, user_id.as_str(), &transaction_value.file_tuuids).await?;
        models.extend(operations);
    }

    if transaction_value.directory_tuuids.len() > 0 {
        // Recover directories
        let filtre_directory = doc!{CHAMP_TUUID: {"$in": &transaction_value.directory_tuuids}, CHAMP_USER_ID: &user_id};
        let ops = doc!{
            "$set": { FIELD_DELETED: false, FIELD_DELETED_INDIRECT: false },
            "$currentDate": { FIELD_DATE_MODIFIED: true },
        };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection.namespace())
                    .filter(filtre_directory)
                    .update(ops)
                    .build()
            )
        );

        // Recover files directly under each directory (indirectly deleted)
        let filtre_files = doc!{
            FIELD_PATH_CUUIDS_0: {"$in": &transaction_value.directory_tuuids},
            FIELD_USER_ID: &user_id,
            FIELD_DELETED: true,
            FIELD_DELETED_INDIRECT: true,
        };
        let mut cursor = collection.find(filtre_files).await?;
        let mut file_tuuids = Vec::new();
        while cursor.advance().await? {
            let row = cursor.deserialize_current()?;
            file_tuuids.push(row.tuuid.to_string());
        }

        // Process files in batches
        for file_batch in file_tuuids.chunks(100) {
            let operations = recycle_files(mongo, user_id.as_str(), file_batch).await?;
            models.extend(operations);
        }
    }

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);
    Ok(aggregator)
}

async fn recycle_files<M>(middleware: &M, user_id: &str, files: &[String]) -> Result<Vec<WriteModel>, CommonError>
where M: MongoDaoTyped
{
    let collection = middleware.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let collection_versions = middleware.get_collection(NOM_COLLECTION_VERSIONS)?;

    let mut models = Vec::new();

    // Rebuild the link from version to file.
    let filtre_filerep = doc!{CHAMP_TUUID: {"$in": files}, CHAMP_USER_ID: &user_id};
    let mut cursor = collection.find(filtre_filerep.clone()).await?;
    while cursor.advance().await? {
        let row = cursor.deserialize_current()?;
        if let Some(fuuids) = row.fuuids_versions {
            let filtre = doc! {
                CHAMP_FUUID: {"$in": fuuids},
            };
            let ops = doc!{
                "$addToSet": { CHAMP_TUUIDS: row.tuuid },
                "$currentDate": { FIELD_DATE_MODIFIED: true },
            };
            models.push(
                WriteModel::UpdateMany(
                    UpdateManyModel::builder()
                        .namespace(collection_versions.namespace())
                        .filter(filtre)
                        .update(ops)
                        .build()
                )
            );
        }
    }

    // Mark all files as no longer deleted.
    let ops = doc! {
        "$set": {
            CHAMP_SUPPRIME: false,
            CHAMP_SUPPRIME_INDIRECT: false
        },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    models.push(
        WriteModel::UpdateMany(
            UpdateManyModel::builder()
                .namespace(collection.namespace())
                .filter(filtre_filerep)
                .update(ops)
                .build()
        )
    );

    Ok(models)
}

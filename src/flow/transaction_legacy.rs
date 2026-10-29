// Support for older transactions no longer used through commands
// Needed for restoration of backups (redo-log).

use std::collections::{HashMap, HashSet};
use millegrilles_common_rust::{bson, serde_json};
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::Utc;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::v3::models::{ErrorMessage, TransactionOperationAggregator, TransactionWrapper};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::hachages::hacher_bytes;
use millegrilles_common_rust::mongodb::options::{Hint, UpdateManyModel, WriteModel};
use millegrilles_common_rust::multibase::Base;
use millegrilles_common_rust::multihash::Code;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::impls::rabbitmq_consumer::DeliveryInfo;
use serde::{Deserialize, Serialize};
use crate::constants::{DOMAIN_NAME, TRANSACTION_DELETE_V3, TRANSACTION_MOVE_V2};
use crate::external::mongo::*;
use crate::external::mongo_struct::{ContactRow, MediaOwnedRow, NodeFichierRepOwned, NodeFichierVersionOwned};
use crate::flow::directories::{get_path_cuuid, RowRepertoirePaths, parse_selection_directories};
use crate::flow::file_events::CollectionEventAggregator;
use crate::flow::transaction_commands::{process_transaction, TransactionAjouterFichiersCollection, TransactionDeplacerFichiersCollection};
use crate::flow::transaction_models::{TransactionDeleteV3, TransactionMoveV2};
use crate::flow::transactions::{delete_file_reps_v3, move_files_collections};
use crate::models::TypeNode;

pub async fn legacy_deplacer_fichiers_collection<M>(
    mongo: &M,
    mut wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped {
    debug!("legacy_deplacer_fichiers_collection id:{}", wrapper.message.id);
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(id) => id,
        None => return Err(CommonError::Str("User_id missing from certificate in new_collection transaction"))
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
    if let Err(e) = dupliquer_structure_repertoires(
        mongo,
        uuid_transaction,
        &transaction_collection.cuuid,
        &transaction_collection.inclure_tuuids,
        user_id_origine.as_str(),
        user_id_destination,
    ).await {
        Err(format!("grosfichiers.transaction_ajouter_fichiers_collection Erreur dupliquer_structure_repertoires sur transaction : {:?}", e))?
    }

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.legacy = true;  // Forces a commit after
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
) -> Result<(), CommonError> where M: MongoDaoTyped {
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
            let result = collection_reps
                .update_one(filtre, ops)
                .upsert(true)
                .await?;
            if result.upserted_id.is_none() {
                info!("dupliquer_structure_repertoires Erreur, aucune valeur upserted pour tuuid {}", tuuid_src);
            }

            match type_node {
                TypeNode::Fichier => {
                    if user_id_destination != user_id {
                        // Copier les versions des fichiers vers le user_id destination
                        let filtre = doc!{CHAMP_TUUIDS: &tuuid_src};
                        let ops = doc!{"$addToSet": {"tuuids": &nouveau_tuuid}, "$currentDate": {FIELD_DATE_MODIFIED: true}};
                        collection_versions.update_many(filtre, ops).await?;

                        if let Some(fuuids_versions) = fichier_rep.fuuids_versions.clone() {
                            copy_media_file(mongo, user_id_destination, user_id, fuuids_versions).await?;
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

    Ok(())
}

async fn copy_media_file<M>(
    middleware: &M,
    user_id: &str,
    original_user_id: &str,
    fuuids_versions: Vec<String>
) -> Result<(), CommonError> where M: MongoDaoTyped {
    debug!("copy_media_file fuuids_versions: {:?}, user_id: {:?}", fuuids_versions, user_id);
    let collection_media = middleware.get_collection_typed::<MediaOwnedRow>(NOM_COLLECTION_MEDIA)?;

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
        collection_media
            .update_one(filtre, ops)
            .upsert(true)
            .await?;
    }
    Ok(())
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
    mut wrapper: TransactionWrapper,
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
        let row = cursor.deserialize_current()?;
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

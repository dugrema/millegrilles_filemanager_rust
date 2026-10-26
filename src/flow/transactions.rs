use crate::constants::*;
use crate::external::mongo::*;
use crate::external::mongo_struct::{FileComment, MediaOwnedRow, MediaWebSubtitleDetail, NodeFichierRepOwned, NodeFichierVersionOwned, VideoDetail};
use crate::external::mq::*;
use crate::flow::directories::get_path_cuuid;
use crate::flow::transaction_models::*;
use crate::models::TypeNode;
use millegrilles_common_rust::async_trait::async_trait;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageMilleGrillesOwned;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoImpl, MongoDaoTyped};
use millegrilles_common_rust::mongodb::ClientSession;
use millegrilles_common_rust::mongodb::options::{DeleteManyModel, DeleteOneModel, InsertOneModel, UpdateManyModel, UpdateOneModel, WriteModel};
use millegrilles_common_rust::serde_json::Value;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::debug;
use millegrilles_common_rust::v3::impls::transaction_service::TransactionServiceImpl;
use millegrilles_common_rust::v3::models::*;
use millegrilles_common_rust::v3::{ConfigService, FormatService, TransactionRouter, TransactionService};
use millegrilles_common_rust::bson;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use crate::flow::file_jobs::set_flag_image_traitee;

pub struct FileManagerTransactionService {
    pub transaction: Arc<dyn TransactionService>,
}

impl FileManagerTransactionService {
    pub fn new(
        config: Arc<dyn ConfigService>,
        format: Arc<dyn FormatService>,
        mongo: Arc<MongoDaoImpl>,
        _restoring: bool,
    ) -> Self {
        let router = FileManagerTransactionRouter { mongo: mongo.clone() };
        let service = TransactionServiceImpl::new(
            config,
            format,
            mongo,
            COLLECTION_NAME_REDOLOG.to_string(),
            COLLECTION_NAME_TRACKING.to_string(),
            Box::new(router),
        );

        Self { transaction: Arc::new(service) }
    }

    pub async fn process_transaction(&self, wrapper: TransactionWrapper, session: Option<&mut ClientSession>) -> Result<(), CommonError> {
        self.transaction.process_transaction(wrapper, session).await
    }

    pub async fn process_value(&self, domain: &str, action: &str, value: Value, session: Option<&mut ClientSession>) -> Result<String, CommonError> {
        self.transaction.process_value(domain, action, value, session).await
    }
}

struct FileManagerTransactionRouter {
    mongo: Arc<MongoDaoImpl>,
    // ignore_duplicates: bool,
}

#[async_trait]
impl TransactionRouter for FileManagerTransactionRouter {
    async fn route(
        &self,
        action: String,
        wrapper: TransactionWrapper
    ) -> Result<TransactionOperationAggregator, CommonError> {
        match action.as_str() {
            // Adding, updating files
            TRANSACTION_NOUVELLE_COLLECTION => new_collection(self.mongo.as_ref(), wrapper, true).await,
            TRANSACTION_NOUVELLE_VERSION => new_file_version(self.mongo.as_ref(), wrapper, true).await,
            TRANSACTION_DECRIRE_FICHIER => update_file_metadata(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_DECRIRE_COLLECTION => update_collection_metadata(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_UPDATE_FILE_TEXT_CONTENT => update_file_text_content(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_DELETE_FILE_COMMENT => delete_file_comment(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_FILE_SUMMARY => file_summary(self.mongo.as_ref(), wrapper).await,

            // Adding, updating internal only (no command)
            TRANSACTION_NOUVELLE_COLLECTION_V2 => new_collection(self.mongo.as_ref(), wrapper, false).await,
            TRANSACTION_NOUVELLE_VERSION_V2 => new_file_version(self.mongo.as_ref(), wrapper, false).await,

            // Moving files
            TRANSACTION_RECYCLE_ITEMS_V4 => recycle_items(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_DELETE_V3 => delete_file_reps_v3(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_MOVE_V2 => move_files_collections(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_COPY_V2 => todo!(),
            TRANSACTION_SUPPRIMER_ORPHELINS => todo!(),
            TRANSACTION_PERMANENTLY_DELETE_FILES => todo!(),

            // Media
            TRANSACTION_ASSOCIER_VIDEO => associate_video_conversion(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_ASSOCIER_CONVERSIONS => associate_file_conversions(self.mongo.as_ref(), wrapper, true).await,
            TRANSACTION_SUPPRIMER_VIDEO_V2 => delete_video(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_IMAGE_SUPPRIMER_JOB_V2 => delete_image_job(self.mongo.as_ref(), wrapper).await,
            COMMAND_VIDEO_DELETE_JOB_V2 => delete_video_job(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_ADD_WEB_SUBTITLE => add_web_subtitle(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_REMOVE_WEB_SUBTITLE => remove_web_subtitle(self.mongo.as_ref(), wrapper).await,

            // Media - internal only (no command)
            TRANSACTION_ASSOCIER_CONVERSIONS_V2 => associate_file_conversions(self.mongo.as_ref(), wrapper, false).await,

            // Sharing
            TRANSACTION_AJOUTER_CONTACT_LOCAL => share_add_contact(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_SUPPRIMER_CONTACTS => share_remove_contact(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_PARTAGER_COLLECTIONS => share_add_collection(self.mongo.as_ref(), wrapper).await,
            TRANSACTION_SUPPRIMER_PARTAGE_USAGER => share_remove_collection(self.mongo.as_ref(), wrapper).await,

            // Legacy but still required (no command associated)
            TRANSACTION_AJOUTER_FICHIERS_COLLECTION => todo!(),
            TRANSACTION_RECUPERER_DOCUMENTS => todo!(),
            TRANSACTION_DELETE_V2 => todo!(),
            TRANSACTION_SUPPRIMER_DOCUMENTS => todo!(),
            TRANSACTION_SUPPRIMER_VIDEO => todo!(),
            TRANSACTION_RECYCLE_ITEMS_V3 => todo!(),
            TRANSACTION_DEPLACER_FICHIERS_COLLECTION => todo!(),
            TRANSACTION_RECUPERER_DOCUMENTS_V2 => todo!(),

            // Obsolete transactions - keep to avoid getting an error on rebuild
            TRANSACTION_IMAGE_SUPPRIMER_JOB => obsolete(),
            TRANSACTION_VIDEO_SUPPRIMER_JOB => obsolete(),
            TRANSACTION_COPIER_FICHIER_TIERS => obsolete(),
            TRANSACTION_ARCHIVER_DOCUMENTS => obsolete(),

            _ => Err(CommonError::Str("Unknown transaction action"))
        }
    }

    fn is_must_flush(&self, message: &MessageMilleGrillesOwned) -> bool {
        let action = match message.routage.as_ref() {
            Some(routing) => match routing.action.as_ref() {
                Some(action) => action.as_str(),
                None => return false
            },
            None => return false
        };

        // Use the action to match all legacy transactions that run database lookups. This prevents
        // proper batching, all previous transactions must be flush before running the following
        // action types.
        match action {
            TRANSACTION_NOUVELLE_COLLECTION
            | TRANSACTION_NOUVELLE_VERSION
            | TRANSACTION_ASSOCIER_CONVERSIONS => true,
            _ => false
        }
    }
}

fn obsolete() -> Result<TransactionOperationAggregator, CommonError> {
    // Obsolete - no effect
    Ok(TransactionOperationAggregator::new())
}

async fn new_collection(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
    legacy_v1: bool,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionNouvelleCollection = wrapper.message.deserialize()?;

    let user_id = match transaction.user_id {
        Some(user_id) => {
            // New type, ensure the certificate is from filemanager
            if legacy_v1 || ! wrapper.certificate.verifier_domaines(vec![DOMAIN_NAME.to_string()])? {
                return Err(CommonError::Str("Wrong domain, new_collection transaction with user_id not allowed"))
            }
            user_id
        },
        None => match wrapper.certificate.get_user_id()? {
            Some(user_id) => user_id,
            None => return Err(CommonError::Str("User_id missing from certificate in new_collection transaction"))
        }
    };

    // Prepare field data
    let tuuid = wrapper.message.id.clone();
    let transaction_date = &wrapper.message.estampille;
    let type_node: &str = match transaction.favoris {
        Some(true) => TypeNode::Collection.into(),
        _ => TypeNode::Repertoire.into(),
    };

    let path_cuuids = match transaction.cuuids {
        Some(path_cuuids) => Some(path_cuuids),  // New type, path already fetched
        None => match transaction.cuuid {
            Some(cuuid) => {
                // Old style, this will fail on restore if all previous transactions are not flushed
                if ! legacy_v1 {
                    return Err(CommonError::Str("Not supported for nouvelleCollectionV2"))
                }
                get_path_cuuid(mongo, &cuuid).await?
            },
            None => None
        }
    };

    let row = NodeFichierRepOwned {
        tuuid: tuuid.clone(),
        user_id: user_id.clone(),
        type_node: type_node.to_string(),
        supprime: false,
        supprime_indirect: false,
        metadata: transaction.metadata,
        flag_index: false,
        flag_rag: false,
        flag_summary: false,
        mimetype: None,
        fuuids_versions: None,
        path_cuuids,
        modified:Utc::now(),
        created: transaction_date.to_owned(),
    };

    // Populate aggregator
    let insertion = BatchInsertions::new(
        NOM_COLLECTION_FICHIERS_REP,
        vec![bson::serialize_to_document(&row)?]
    );
    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.batch_insertions = Some(vec![insertion]);

    // Note: No need to put the legacy flag, this is only used to force a flush after the transaction is ready.
    //       This transaction needs the flush to happen before it runs (for get_path_cuuid)
    // aggregator.legacy = legacy_v1;

    Ok(aggregator)
}

async fn new_file_version<M>(
    mongo: &M,
    wrapper: TransactionWrapper,
    legacy_v1: bool,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped {
    let transaction_value: TransactionNouvelleVersion = wrapper.message.deserialize()?;

    let user_id = match transaction_value.user_id {
        Some(user_id) => {
            // New type, ensure the certificate is from filemanager
            if legacy_v1 || ! wrapper.certificate.verifier_domaines(vec![DOMAIN_NAME.to_string()])? {
                return Err(CommonError::Str("Wrong domain, new_collection transaction with user_id not allowed"))
            }
            user_id
        },
        None => match wrapper.certificate.get_user_id()? {
            Some(user_id) => user_id,
            None => return Err(CommonError::Str("User_id missing from certificate in new_collection transaction"))
        }
    };

    let tuuid = match transaction_value.tuuid {
        Some(tuuid) => tuuid,
        None => wrapper.message.id.clone()
    };

    let mimetype = transaction_value.mimetype;
    let (
        flag_media_traite,
        flag_video_traite,
        flag_media,
        flag_summary
    ) = get_flags_media(mimetype.as_str());

    let mut visites = HashMap::new();
    visites.insert("nouveau".to_string(), Utc::now());
    let last_visit_verification = DateTime::from_timestamp(1704085200, 0).expect("from_timestamp");

    let path_cuuids = match transaction_value.path_cuuids {
        Some(path_cuuids) => path_cuuids,
        None => {
            if ! legacy_v1 {
                return Err(CommonError::Str("path_cuuids required for nouvelleCollectionV2"))
            }
            match get_path_cuuid(mongo, transaction_value.cuuid.as_str()).await? {
                Some(inner) => inner,
                None => return Err(CommonError::Str("Unknown directory (cuuid not found)"))
            }
        }
    };

    let now = Utc::now();
    let created = wrapper.message.estampille.clone();
    let file_size = transaction_value.taille as i64;

    let fichier_version_row = NodeFichierVersionOwned {
        fuuid: transaction_value.fuuid.clone(),
        mimetype: mimetype.clone(),
        taille: transaction_value.taille,
        tuuids: vec![tuuid.clone()],
        fuuids_reclames: vec![transaction_value.fuuid.clone()],
        language: None,
        visites,
        last_visit_verification,
        created: created.clone(),
        modified: now.clone(),
        flag_media,
        flag_media_retry: None,
        flag_media_traite,
        flag_video_traite,
        cle_id: transaction_value.cle_id,
        format: transaction_value.format,
        nonce: transaction_value.nonce,
        verification: transaction_value.verification,
    };

    let fichier_rep_row = NodeFichierRepOwned {
        tuuid,
        user_id: user_id.clone(),
        type_node: TypeNode::Fichier.to_str().to_string(),
        supprime: false,
        supprime_indirect: false,
        metadata: transaction_value.metadata,
        flag_index: false,
        flag_rag: false,
        flag_summary,
        mimetype: Some(mimetype),
        fuuids_versions: Some(vec![transaction_value.fuuid.clone()]),
        path_cuuids: Some(path_cuuids),
        modified: now,
        created,
    };

    let mut aggregator = TransactionOperationAggregator::new();

    // Insert rep and version rows
    let mut insertion_batches = Vec::new();
    insertion_batches.push(BatchInsertions {
        collection_name: NOM_COLLECTION_VERSIONS.to_string(),
        insertions: vec![bson::serialize_to_document(&fichier_version_row)?]
    });
    insertion_batches.push(BatchInsertions {
        collection_name: NOM_COLLECTION_FICHIERS_REP.to_string(),
        insertions: vec![bson::serialize_to_document(&fichier_rep_row)?]
    });

    aggregator.batch_insertions = Some(insertion_batches);

    // Update quota
    let collection = mongo.get_collection(NOM_COLLECTION_QUOTAS_USAGERS)?;
    let filter = doc!{ FIELD_USER_ID: &user_id };
    let ops = doc!{
        "$setOnInsert": { FIELD_DATE_CREATED: bson::datetime::DateTime::now() },
        "$inc": { "bytes_total_versions": file_size, "nombre_total_versions": 1 },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };
    let update_model_versions = WriteModel::UpdateOne(
        UpdateOneModel::builder()
            .upsert(true)
            .namespace(collection.namespace())
            .filter(filter)
            .update(ops)
            .build()
    );
    aggregator.unordered = Some(vec![update_model_versions]);

    Ok(aggregator)
}

async fn associate_file_conversions<M>(
    mongo: &M,
    wrapper: TransactionWrapper,
    legacy_v1: bool,
) -> Result<TransactionOperationAggregator, CommonError> where M: MongoDaoTyped {
    let transaction: TransactionAssocierConversions = wrapper.message.deserialize()?;

    // Mapper tous les fuuids
    let fuuids_reclames = {
        let mut fuuids_reclames = Vec::new();
        for (_, image) in transaction.images.iter() {
            if image.data_chiffre.is_none() {  // Ignore content with data: no file to manage
                fuuids_reclames.push(image.hachage.to_owned());
            }
        }
        fuuids_reclames
    };

    let user_ids = if legacy_v1 {
        // Fetch the affected user_ids
        let filtre_reps = doc! { FIELD_FUUIDS_VERSIONS: &transaction.fuuid };
        let collection_reps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
        let mut user_ids = HashSet::with_capacity(20);
        let mut cursor = collection_reps.find(filtre_reps).await?;
        while let Some(row) = cursor.next().await {
            user_ids.insert(row?.user_id);
        }
        if user_ids.is_empty() {
            return Err(CommonError::String(format!("transaction_associer_conversions No match for fuuid {}", transaction.fuuid)))
        }
        user_ids.into_iter().collect::<Vec<String>>()
    } else {
        // V2: User ids must be provided in the transaction
        match transaction.user_ids {
            Some(user_ids) => user_ids,
            None => return Err(CommonError::Str("User ids not provided"))
        }
    };

    let created = &wrapper.message.estampille;

    let mut updates = Vec::with_capacity(20);

    // Insert media
    let collection_media = mongo.get_collection(NOM_COLLECTION_MEDIA)?;
    for user_id in user_ids {
        // Map with the struct (acts as data validation)
        let media_row = MediaOwnedRow {
            fuuid: transaction.fuuid.clone(),
            user_id: user_id.clone(),
            created: created.clone(),
            modified: Utc::now(),
            height: transaction.height,
            width: transaction.width,
            duration: transaction.duration,
            video_codec: transaction.video_codec.clone(),
            anime: transaction.anime.unwrap_or(false),
            images: None,
            video: None,
            audio: None,
            subtitles: None,
            web_subtitles: None,
        };

        let mut set_ops = doc! {
            "height": media_row.height,
            "width": media_row.width,
            "duration": media_row.duration,
            "videoCodec": media_row.video_codec,
            "anime": media_row.anime,
        };

        if transaction.images.len() > 0 {
            for (key, value) in &transaction.images {
                set_ops.insert(format!("images.{}", key), bson::serialize_to_document(value)?);
            }
        };

        if let Some(audio) = transaction.audio.as_ref() {
            set_ops.insert("audio", bson::serialize_to_bson(audio)?);
        }
        if let Some(subtitles) = transaction.subtitles.as_ref() {
            set_ops.insert("subtitles", bson::serialize_to_bson(subtitles)?);
        }

        // Insert into the media table.
        let filtre_media = doc! {"fuuid": media_row.fuuid, "user_id": media_row.user_id};
        let ops = doc! {
                "$set": set_ops,
                "$setOnInsert": { FIELD_DATE_CREATED: Utc::now() },
                "$currentDate": { FIELD_DATE_MODIFIED: true },
        };

        let update_model_media = WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .upsert(true)
                .namespace(collection_media.namespace())
                .filter(filtre_media)
                .update(ops)
                .build()
        );
        updates.push(update_model_media);
    }

    // Update the versions table
    let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let ops = doc! {
        "$set": { CHAMP_FLAG_MEDIA_TRAITE: true },
        "$addToSet": { CHAMP_FUUIDS_RECLAMES: {"$each": &fuuids_reclames} },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    let filtre_versions = doc!{ FIELD_FUUID: &transaction.fuuid };
    let update_model_versions = WriteModel::UpdateOne(
        UpdateOneModel::builder()
            .upsert(true)
            .namespace(collection_versions.namespace())
            .filter(filtre_versions)
            .update(ops)
            .build()
    );
    updates.push(update_model_versions);

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(updates);

    Ok(aggregator)
}

async fn update_file_metadata(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionDecrireFichier = wrapper.message.deserialize()?;

    let user_id = wrapper.certificate.get_user_id()?;

    let tuuid = transaction.tuuid.as_str();
    let mut filter_reps = doc! { CHAMP_TUUID: tuuid };
    if let Some(inner) = &user_id {
        filter_reps.insert("user_id", inner);
    }

    let mut set_ops = doc! {
        CHAMP_FLAG_INDEX: false,
    };

    let mut updates = Vec::with_capacity(3);

    // Modifier metadata
    if let Some(metadata) = transaction.metadata {
        let metadata_bson = match bson::serialize_to_document(&metadata) {
            Ok(inner) => inner,
            Err(e) => Err(format!("transactions.transaction_decire_fichier Erreur conversion metadata vers bson : {:?}", e))?
        };
        set_ops.insert(FIELD_METADATA, metadata_bson);
    }

    if let Some(mimetype) = transaction.mimetype.as_ref() {
        set_ops.insert(FIELD_MIMETYPE, mimetype);

        // Update versions
        let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
        let filter = doc!{ FIELD_TUUIDS: tuuid };
        let ops = doc!{ "$set": { FIELD_MIMETYPE: mimetype }, "$currentDate": { FIELD_DATE_MODIFIED: true } };
        let update_model_versions = WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .upsert(true)
                .namespace(collection_versions.namespace())
                .filter(filter)
                .update(ops)
                .build()
        );
        updates.push(update_model_versions);
    }

    // Creer job indexation
    let ops = doc! {
        "$set": set_ops,
        "$unset": { CHAMP_FLAG_INDEX_RETRY: true, CHAMP_FLAG_INDEX_ERREUR: true },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let update_model_versions = WriteModel::UpdateOne(
        UpdateOneModel::builder()
            .upsert(true)
            .namespace(collection_reps.namespace())
            .filter(filter_reps)
            .update(ops)
            .build()
    );
    updates.push(update_model_versions);

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(updates);

    Ok(aggregator)
}

async fn update_collection_metadata(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionDecrireCollection = wrapper.message.deserialize()?;

    let tuuid = transaction.tuuid.as_str();
    let filter = doc! { CHAMP_TUUID: tuuid };

    let doc_metadata = match bson::serialize_to_document(&transaction.metadata) {
        Ok(d) => d,
        Err(e) => Err(format!("transactions.transaction_decrire_collection Erreur conversion transaction : {:?}", e))?
    };

    let set_ops = doc! {
        "metadata": doc_metadata,
        CHAMP_FLAG_INDEX: false,
    };

    let ops = doc! {
        "$set": set_ops,
        "$unset": { CHAMP_FLAG_INDEX_RETRY: true, CHAMP_FLAG_INDEX_ERREUR: true },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };

    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let update_model_versions = WriteModel::UpdateOne(
        UpdateOneModel::builder()
            .upsert(true)
            .namespace(collection.namespace())
            .filter(filter)
            .update(ops)
            .build()
    );
    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(vec![update_model_versions]);

    Ok(aggregator)
}

async fn update_file_text_content(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionUpdateFileTextContent = wrapper.message.deserialize()?;

    let user_id = match wrapper.certificate.get_user_id()? {
        Some(inner) => inner,
        None => Err(format!("transaction_update_file_text_content Missing user_id from certificate for id:{}", wrapper.message.id))?
    };

    let tuuid = transaction.tuuid.as_str();
    let created = bson::datetime::DateTime::from_chrono(wrapper.message.estampille.clone());

    let collection_comments = mongo.get_collection(NOM_COLLECTION_FILE_COMMENTS)?;

    let mut models = Vec::with_capacity(5);

    if let Some(tags) = &transaction.tags {
        // Insert tags
        let filter = doc!{ FIELD_TUUID: &tuuid, "comment_id": CONST_COMMENT_TAGS };
        let ops = doc!{
                "$set": {
                    "date": &created,
                    "encrypted_data": bson::serialize_to_document(&tags)?,
                }
            };
        let update_model = WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .upsert(true)
                .namespace(collection_comments.namespace())
                .filter(filter)
                .update(ops)
                .build()
        );
        models.push(update_model);
    }

    if let Some(comment) = transaction.comment {
        match transaction.comment_id {
            Some(comment_id) => {
                // Update
                let filter = doc!{ FIELD_TUUID: &tuuid, "comment_id": comment_id };
                let ops = doc!{
                    "$set": {
                        "date": &created,
                        "encrypted_data": bson::serialize_to_document(&comment)?,
                    }
                };
                let update_model = WriteModel::UpdateOne(
                    UpdateOneModel::builder()
                        .upsert(true)
                        .namespace(collection_comments.namespace())
                        .filter(filter)
                        .update(ops)
                        .build()
                );
                models.push(update_model);
            }
            None => {
                // Insert
                let comment_entry = FileComment {
                    tuuid: tuuid.to_string(),
                    comment_id: wrapper.message.id.clone(),
                    encrypted_data: comment,
                    date: wrapper.message.estampille.clone(),
                    user_id: Some(user_id.clone()),
                };
                let insert_model = WriteModel::InsertOne(
                    InsertOneModel::builder()
                        .namespace(collection_comments.namespace())
                        .document(bson::serialize_to_document(&comment_entry)?)
                        .build()
                );
                models.push(insert_model);
            }
        }
    }

    // Update file modification date has hint to change during sync, set index flag to false to re-index the file
    let filter = doc! {"tuuid": tuuid, "user_id": &user_id};
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let ops = doc! {
        "$set": { CHAMP_FLAG_INDEX: false },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };
    let update_model = WriteModel::UpdateOne(
        UpdateOneModel::builder()
            .upsert(true)
            .namespace(collection.namespace())
            .filter(filter)
            .update(ops)
            .build()
    );
    models.push(update_model);

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);

    Ok(aggregator)
}

async fn delete_file_comment(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionDeleteFileComment = wrapper.message.deserialize()?;

    let user_id = match wrapper.certificate.get_user_id()? {
        Some(inner) => inner,
        None => return Err(CommonError::Str("Missing user_id"))
    };

    let mut models = Vec::with_capacity(2);

    let collection_comments = mongo.get_collection(NOM_COLLECTION_FILE_COMMENTS)?;
    let filter = doc!{ FIELD_TUUID: &transaction.tuuid, "comment_id": transaction.comment_id };

    models.push(
        WriteModel::DeleteOne(
            DeleteOneModel::builder()
                .namespace(collection_comments.namespace())
                .filter(filter)
                .build()
        )
    );

    // Update file modification date has hint to change during sync, set index flag to false to re-index the file
    let filter = doc! { FIELD_TUUID: &transaction.tuuid, FIELD_USER_ID: &user_id};
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let ops = doc! {
        "$set": {CHAMP_FLAG_INDEX: false},
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };
    models.push(
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection.namespace())
                .filter(filter)
                .update(ops)
                .build()
        )
    );

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);

    Ok(aggregator)
}

async fn associate_video_conversion(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionAssocierVideo = wrapper.message.deserialize()?;

    let created = bson::DateTime::from_chrono(wrapper.message.estampille.clone());

    let video_detail = VideoDetail {
        fuuid: transaction.fuuid.clone(),
        fuuid_video: transaction.fuuid_video.clone(),
        taille_fichier: transaction.taille_fichier,
        mimetype: transaction.mimetype.clone(),
        codec: transaction.codec.clone(),
        cle_conversion: transaction.cle_conversion.clone(),
        width: transaction.width,
        height: transaction.height,
        bitrate: transaction.bitrate,
        quality: transaction.quality,
        audio_stream_idx: transaction.audio_stream_idx,
        subtitle_stream_idx: transaction.subtitle_stream_idx,
        header: transaction.header,
        format: transaction.format,
        nonce: transaction.nonce,
        cle_id: transaction.cle_id,
    };

    let cle_video = match transaction.cle_conversion {
        Some(inner) => inner,  // Nouveau avec 2023.7.4
        None => {
            // Note : avant 2023.7.4 (et media 2023.7.4), la cle_video n'etait pas fournie
            //        cause un probleme avec les videos verticaux.
            let resolution = match transaction.height {
                Some(height) => match transaction.width {
                    Some(width) => {
                        // La resolution est le plus petit des deux nombres
                        if width < height {
                            width
                        } else {
                            height
                        }
                    },
                    None => height,
                },
                None => 0
            };
            let bitrate_quality = {
                match &transaction.quality {
                    Some(q) => q.to_owned(),
                    None => match &transaction.bitrate {
                        Some(b) => b.to_owned() as i32,
                        None => 0
                    }
                }
            };
            format!("{};{};{}p;{}", &transaction.mimetype, &transaction.codec, resolution, bitrate_quality)
        }
    };

    let mut models = Vec::with_capacity(2);

    let filter = doc!{ FIELD_FUUID: &transaction.fuuid, FIELD_USER_ID: &transaction.user_id };
    let collection_media = mongo.get_collection(NOM_COLLECTION_MEDIA)?;
    let ops = doc!{
        "$set": {"anime": true, format!("video.{}", cle_video): bson::serialize_to_document(&video_detail)?},
        "$setOnInsert": { FIELD_DATE_CREATED: &created },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    models.push(
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .upsert(true)
                .namespace(collection_media.namespace())
                .filter(filter)
                .update(ops)
                .build()
        )
    );

    // MAJ de la version du fichier
    let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let filter = doc!{ FIELD_FUUID: transaction.fuuid };
    let ops = doc! {
        "$set": { CHAMP_FLAG_VIDEO_TRAITE: true },
        "$addToSet": { CHAMP_FUUIDS_RECLAMES: transaction.fuuid_video },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    models.push(
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection_versions.namespace())
                .filter(filter)
                .update(ops)
                .build()
        )
    );

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);

    Ok(aggregator)
}

async fn delete_video_job(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionSupprimerJobVideoV2 = wrapper.message.deserialize()?;

    let filtre_video = doc! { FIELD_FUUID: &transaction.fuuid, FIELD_TUUID: &transaction.tuuid };

    // Set flag versionFichiers
    let collection = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let ops = doc! {
        "$set": {
            CHAMP_FLAG_VIDEO_TRAITE: true,
            CHAMP_FLAG_MEDIA_TRAITE: true,  // legacy pour eviter de laisser un flag actif pre release-2024.9
        },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.unordered = Some(vec![
        WriteModel::UpdateMany(
            UpdateManyModel::builder()
                .namespace(collection.namespace())
                .filter(filtre_video)
                .update(ops)
                .build()
        )
    ]);

    Ok(aggregator)
}

async fn delete_file_reps_v3(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionDeleteV3 = wrapper.message.deserialize()?;

    let collection_fichiersrep = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let collection_fichiersversion = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;

    let mut models = Vec::with_capacity(6);

    // Start with subfiles - they get removed from the versions collectino only,
    // the directories handling already covers the filerep collection.
    if let Some(files) = transaction.subfiles {
        debug!("transaction_delete_v3 Delete subfiles: {:?}", files);
        // Mark all listed files as deleted.

        let version_filtre = doc!{ FIELD_TUUIDS: { "$in": &files } };
        let version_ops = doc!{"$pullAll": { FIELD_TUUIDS: &files }, "$currentDate": { FIELD_DATE_MODIFIED: true } };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection_fichiersversion.namespace())
                    .filter(version_filtre)
                    .update(version_ops)
                    .build()
            )
        );

        // Note : already handled in the directories (same collection)
        // let file_filtre = doc!{ FIELD_TUUID: {"$in": &files} };
        // let rep_ops = doc!{
        //     "$set": { FIELD_DELETED: true, FIELD_DELETED_INDIRECT: true, CHAMP_FLAG_INDEX: false},
        //     "$currentDate": { FIELD_DATE_MODIFIED: true }
        // };
        // models.push(
        //     WriteModel::UpdateMany(
        //         UpdateManyModel::builder()
        //             .namespace(collection_fichiersrep.namespace())
        //             .filter(file_filtre)
        //             .update(rep_ops)
        //             .build()
        //     )
        // );
    }

    // Handle subdirectories first
    if let Some(subdirectories) = transaction.subdirectories {
        debug!("transaction_delete_v3 Delete subdirectories: {:?}", subdirectories);

        // Mark all the files as indirectly deleted in the rep collection
        let filtre_rep = doc! {
            "path_cuuids.0": {"$in": &subdirectories},
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
        let directory_filtre = doc!{ FIELD_TUUID: { "$in": subdirectories } };
        let directory_ops = doc!{
            "$set": { FIELD_DELETED: true, FIELD_DELETED_INDIRECT: true, CHAMP_FLAG_INDEX: false },
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
    if let Some(directories) = transaction.directories {
        debug!("transaction_delete_v3 Delete directories: {:?}", directories);

        // Mark all the files as indirectly deleted in the rep collection
        let filtre_rep = doc! {
            "path_cuuids.0": {"$in": &directories},
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
        let directory_filtre = doc!{ FIELD_TUUID: {"$in": directories} };
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

    if let Some(files) = transaction.files {
        debug!("transaction_delete_v3 Delete files: {:?}", files);
        // Mark all listed files as deleted.

        let version_filtre = doc!{ FIELD_TUUIDS: {"$in": &files} };
        let version_ops = doc!{"$pullAll": { FIELD_TUUIDS: &files }, "$currentDate": { FIELD_DATE_MODIFIED: true } };
        models.push(
            WriteModel::UpdateMany(
                UpdateManyModel::builder()
                    .namespace(collection_fichiersversion.namespace())
                    .filter(version_filtre)
                    .update(version_ops)
                    .build()
            )
        );

        let file_filtre = doc!{ FIELD_TUUID: { "$in": &files } };
        let rep_ops = doc!{
            "$set": { FIELD_DELETED: true, FIELD_DELETED_INDIRECT: false, CHAMP_FLAG_INDEX: false},
            "$currentDate": { FIELD_DATE_MODIFIED: true }
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

async fn delete_video(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionSupprimerVideoV2 = wrapper.message.deserialize()?;

    let mut ops_unset_video = doc!{};
    for label in transaction.labels {
        ops_unset_video.insert(format!("video.{}", label), true);
    }

    let mut models = Vec::with_capacity(3);

    // Remove video with matching fuuid
    let filtre_media = doc!{ CHAMP_FUUID: &transaction.fuuid, FIELD_USER_ID: &transaction.user_id };
    let ops_media = doc!{ "$unset": ops_unset_video, "$currentDate": { FIELD_DATE_MODIFIED: true } };
    let collection_media = mongo.get_collection(NOM_COLLECTION_MEDIA)?;
    models.push(
        WriteModel::UpdateMany(
            UpdateManyModel::builder()
                .namespace(collection_media.namespace())
                .filter(filtre_media)
                .update(ops_media)
                .build()
        )
    );

    // Cleanup file version (claimed fuuids)
    let filtre_versions = doc!{ FIELD_FUUID: &transaction.fuuid };
    let ops = doc! {
        "$pull": { CHAMP_FUUIDS_RECLAMES: &transaction.fuuid_video },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };
    let collection_version_fichier = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;

    debug!("transaction_supprimer_video Supprimer video {:?} ops {:?}", filtre_versions, ops);
    models.push(
        WriteModel::UpdateMany(
            UpdateManyModel::builder()
                .namespace(collection_version_fichier.namespace())
                .filter(filtre_versions)
                .update(ops)
                .build()
        )
    );

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);

    Ok(aggregator)
}

async fn move_files_collections(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionMoveV2 = wrapper.message.deserialize()?;

    let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;

    let capacity = match transaction.directories.as_ref() {
        Some(inner) => inner.len() + 2,
        None => 2
    };
    let mut models = Vec::with_capacity(capacity);

    if let Some(files) = transaction.files {
        let filtre = doc!{
            FIELD_TUUID: { "$in": files },
            FIELD_NODE_TYPE: TypeNode::Fichier.to_str()
        };
        let ops = doc! {
            "$set": { FIELD_PATH_CUUIDS: &transaction.destination, CHAMP_FLAG_INDEX: false },
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

    if let Some(directories) = transaction.directories {
        for move_command in directories {
            // Move everything directly under the directories (including deleted files/subdirectory)
            for directory in move_command.directories {
                // Move the directory itself
                let filtre = doc! { FIELD_TUUID: &directory };
                // Change type_node to Repertoire if it is Collection. It is possible to move a top-level Collection under another.
                let ops = doc! {
                    "$set": {
                        FIELD_PATH_CUUIDS: &move_command.path,
                        FIELD_NODE_TYPE: TypeNode::Repertoire.to_str(),
                        CHAMP_FLAG_INDEX: false
                    },
                    "$currentDate": { FIELD_DATE_MODIFIED: true }
                };
                models.push(
                    WriteModel::UpdateOne(
                        UpdateOneModel::builder()
                            .namespace(collection_reps.namespace())
                            .filter(filtre)
                            .update(ops)
                            .build()
                    )
                );

                // Move files under directory
                let mut new_path = vec![&directory];
                new_path.extend(&move_command.path);
                let filtre = doc! { "path_cuuids.0": &directory, FIELD_NODE_TYPE: TypeNode::Fichier.to_str() };
                let ops = doc! {
                    "$set": { FIELD_PATH_CUUIDS: new_path, CHAMP_FLAG_INDEX: false },
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
        }
    }

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);

    Ok(aggregator)
}

async fn file_summary(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionFileSummary = wrapper.message.deserialize()?;

    let mut models = Vec::with_capacity(5);

    let created = bson::datetime::DateTime::from_chrono(wrapper.message.estampille.clone());

    if let Some(comment) = transaction.comment {
        let collection_comments = mongo.get_collection(NOM_COLLECTION_FILE_COMMENTS)?;
        // Insert
        let comment_entry = FileComment {
            tuuid: transaction.tuuid.clone(),
            comment_id: wrapper.message.id.clone(),
            encrypted_data: comment.clone(),
            date: wrapper.message.estampille.clone(),
            user_id: None,
        };
        models.push(
            WriteModel::InsertOne(
                InsertOneModel::builder()
                    .namespace(collection_comments.namespace())
                    .document(bson::serialize_to_document(&comment_entry)?)
                    .build()
            )
        );

        // Insert tags when appropriate
        if let Some(tags) = &transaction.tags {
            let filter = doc!{ FIELD_TUUID: &transaction.tuuid, "comment_id": CONST_COMMENT_TAGS };
            let ops = doc!{
                "$set": {
                    "date": &created,
                    "encrypted_data": bson::serialize_to_document(&tags)?,
                }
            };
            models.push(
                WriteModel::UpdateOne(
                    UpdateOneModel::builder()
                        .namespace(collection_comments.namespace())
                        .filter(filter)
                        .update(ops)
                        .upsert(true)
                        .build()
                )
            );

        }
    }

    // Set summary flag to true, reset index flag for reindexing with updated content.
    let filter = doc!{ FIELD_TUUID: &transaction.tuuid };
    let ops = doc!{
        "$set": { CHAMP_FLAG_SUMMARY: true, CHAMP_FLAG_INDEX: false },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    models.push(
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection_reps.namespace())
                .filter(filter)
                .update(ops)
                .build()
        )
    );

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);

    Ok(aggregator)
}

async fn add_web_subtitle(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionAddWebSubtitle = wrapper.message.deserialize()?;
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return Err(CommonError::Str("User_id missing from certificate"))
    };
    let created = bson::datetime::DateTime::from_chrono(wrapper.message.estampille.clone());

    let fuuid = &transaction.file_fuuid.clone();
    let subtitle_fuuid = &transaction.subtitle_fuuid.clone();

    let mut models = Vec::with_capacity(5);

    // Insert the subtitle_fuuid in version_fuuid to ensure it is claimed
    let filter_version = doc!{ FIELD_FUUID: &fuuid };
    let ops = doc!{
        "$addToSet": { CHAMP_FUUIDS_RECLAMES: &subtitle_fuuid },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    models.push(
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection_versions.namespace())
                .filter(filter_version)
                .update(ops)
                .build()
        )
    );

    let subtitle: MediaWebSubtitleDetail = transaction.into();
    let subtitle = bson::serialize_to_document(&subtitle)?;

    let filter_media = doc!{CHAMP_FUUID: fuuid, CHAMP_USER_ID: &user_id};
    let ops = doc!{
        "$setOnInsert": { FIELD_DATE_CREATED: created },
        "$push": {"web_subtitles": subtitle},
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    let collection_media = mongo.get_collection(NOM_COLLECTION_MEDIA)?;
    models.push(
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection_media.namespace())
                .filter(filter_media)
                .update(ops)
                .upsert(true)
                .build()
        )
    );

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);

    Ok(aggregator)
}

async fn remove_web_subtitle(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionRemoveWebSubtitle = wrapper.message.deserialize()?;
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return Err(CommonError::Str("User_id missing from certificate"))
    };

    let fuuid = &transaction.file_fuuid.clone();
    let subtitle_fuuid = &transaction.subtitle_fuuid.clone();

    let mut models = Vec::with_capacity(5);

    let filter_media = doc!{CHAMP_FUUID: fuuid, CHAMP_USER_ID: &user_id};
    let ops = doc!{
        "$pull": { "web_subtitles": {"fuuid": subtitle_fuuid} },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    let collection_media = mongo.get_collection(NOM_COLLECTION_MEDIA)?;
    models.push(
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection_media.namespace())
                .filter(filter_media)
                .update(ops)
                .build()
        )
    );

    // Remove the subtitle_fuuid in version_fuuid to ensure it is no longer claimed
    let filter_version = doc!{CHAMP_FUUID: &fuuid};
    let ops = doc!{
        "$pull": { CHAMP_FUUIDS_RECLAMES: &subtitle_fuuid },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    models.push(
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection_versions.namespace())
                .filter(filter_version)
                .update(ops)
                .build()
        )
    );

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);

    Ok(aggregator)
}

async fn recycle_items(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionRecycleItemsV4 = wrapper.message.deserialize()?;
    let user_id = transaction.user_id;

    let mut models = Vec::with_capacity(20);

    // Restore directories
    let collection_file_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let filter_dirs = doc! {
        FIELD_TUUID: { "$in": &transaction.directory_tuuids },
        FIELD_USER_ID: &user_id,
    };
    let ops_dirs = doc! {
        "$set": { FIELD_DELETED: false, FIELD_DELETED_INDIRECT: false },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };
    models.push(
        WriteModel::UpdateMany(
            UpdateManyModel::builder()
                .namespace(collection_file_reps.namespace())
                .filter(filter_dirs)
                .update(ops_dirs)
                .build()
        )
    );

    // Restore files in fichier_reps
    let mut file_tuuids = Vec::new();
    for tuuids_item in transaction.file_tuuids_by_fuuid.values() {
        file_tuuids.extend(tuuids_item);
    }
    let filter_file_reps = doc! { FIELD_TUUID: { "$in": &file_tuuids }, FIELD_USER_ID: &user_id };
    let ops_file_reps = doc! {
        "$set": { FIELD_DELETED: false, FIELD_DELETED_INDIRECT: false },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };
    models.push(
        WriteModel::UpdateMany(
            UpdateManyModel::builder()
                .namespace(collection_file_reps.namespace())
                .filter(filter_file_reps)
                .update(ops_file_reps)
                .build()
        )
    );

    // Add back the tuuids by fuuids in the versions table
    let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    for (fuuid, tuuids) in &transaction.file_tuuids_by_fuuid {
        let filter_version = doc! { FIELD_FUUID: fuuid };
        let ops_version = doc! {
            "$addToSet": { CHAMP_TUUIDS: { "$each": tuuids } },
            "$currentDate": { FIELD_DATE_MODIFIED: true },
        };
        models.push(
            WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(collection_versions.namespace())
                    .filter(filter_version)
                    .update(ops_version)
                    .build()
            )
        );
    }

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(models);

    Ok(aggregator)
}

async fn delete_image_job(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionSupprimerJobImageV2 = wrapper.message.deserialize()?;

    // Indiquer que la job a ete completee et ne doit pas etre redemarree.
    let filter = doc! { FIELD_FUUID: &transaction.fuuid };
    let collection = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let ops = doc! {
        "$set": { CHAMP_FLAG_MEDIA_TRAITE: true },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(vec![
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection.namespace())
                .filter(filter)
                .update(ops)
                .build()
        )
    ]);

    Ok(aggregator)
}

async fn share_add_contact(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionAjouterContactLocal = wrapper.message.deserialize()?;
    let created = bson::datetime::DateTime::from_chrono(wrapper.message.estampille.clone());

    let filter = doc! {
        CHAMP_CONTACT_ID: &wrapper.message.id,
        CHAMP_USER_ID: &transaction.user_id,
        "contact_user_id": &transaction.contact_user_id,
    };
    let ops = doc! {
        "$setOnInsert": {
            CHAMP_USER_ID: &transaction.user_id,
            "contact_user_id": &transaction.contact_user_id,
            FIELD_DATE_CREATED: &created,
        },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    let collection = mongo.get_collection(NOM_COLLECTION_PARTAGE_CONTACT)?;

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.ordered = Some(vec![
        WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection.namespace())
                .filter(filter)
                .update(ops)
                .upsert(true)
                .build()
        )
    ]);

    Ok(aggregator)
}

async fn share_remove_contact(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionSupprimerContacts = wrapper.message.deserialize()?;
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return Err(CommonError::Str("User_id missing from cetificate"))
    };

    let mut models = Vec::with_capacity(2);

    let filtre = doc! {
        CHAMP_USER_ID: &user_id,
        CHAMP_CONTACT_ID: {"$in": &transaction.contact_ids},
    };
    let collection_contacts = mongo.get_collection(NOM_COLLECTION_PARTAGE_CONTACT)?;
    models.push(
        WriteModel::DeleteMany(
            DeleteManyModel::builder()
                .namespace(collection_contacts.namespace())
                .filter(filtre.clone())
                .build()
        )
    );

    // Remove all shared collections associated to deleted contacts
    let collection_shared_collections = mongo.get_collection(NOM_COLLECTION_PARTAGE_COLLECTIONS)?;
    models.push(
        WriteModel::DeleteMany(
            DeleteManyModel::builder()
                .namespace(collection_shared_collections.namespace())
                .filter(filtre)
                .build()
        )
    );

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.unordered = Some(models);

    Ok(aggregator)
}

async fn share_add_collection(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionPartagerCollections = wrapper.message.deserialize()?;
    let created = bson::datetime::DateTime::from_chrono(wrapper.message.estampille.clone());
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return Err(CommonError::Str("User_id missing from cetificate"))
    };

    let mut models = Vec::with_capacity(transaction.contact_ids.len() + transaction.cuuids.len());

    let collection = mongo.get_collection(NOM_COLLECTION_PARTAGE_COLLECTIONS)?;
    for contact_id in &transaction.contact_ids {
        for cuuid in &transaction.cuuids {
            let filter = doc! {
                CHAMP_USER_ID: &user_id,
                CHAMP_CONTACT_ID: &contact_id,
                CHAMP_TUUID: cuuid,
            };
            let ops = doc! {
                "$setOnInsert": {
                    CHAMP_USER_ID: &user_id,
                    CHAMP_CONTACT_ID: &contact_id,
                    CHAMP_TUUID: cuuid,
                    FIELD_DATE_CREATED: &created,
                },
                "$currentDate": { FIELD_DATE_MODIFIED: true }
            };
            models.push(
                WriteModel::UpdateOne(
                    UpdateOneModel::builder()
                        .namespace(collection.namespace())
                        .filter(filter)
                        .update(ops)
                        .upsert(true)
                        .build()
                )
            );
        }
    }

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.unordered = Some(models);

    Ok(aggregator)
}

async fn share_remove_collection(
    mongo: &dyn MongoDao,
    wrapper: TransactionWrapper,
) -> Result<TransactionOperationAggregator, CommonError> {
    let transaction: TransactionSupprimerPartageUsager = wrapper.message.deserialize()?;
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return Err(CommonError::Str("User_id missing from certificate"))
    };

    let filtre = doc! {
        CHAMP_USER_ID: &user_id,
        CHAMP_CONTACT_ID: transaction.contact_id,
        CHAMP_TUUID: transaction.tuuid,
    };
    let collection = mongo.get_collection(NOM_COLLECTION_PARTAGE_COLLECTIONS)?;

    let mut aggregator = TransactionOperationAggregator::new();
    aggregator.unordered = Some(vec![
        WriteModel::DeleteOne(
            DeleteOneModel::builder()
                .namespace(collection.namespace())
                .filter(filtre)
                .build()
        )
    ]);

    Ok(aggregator)
}

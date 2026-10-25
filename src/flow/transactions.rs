use std::collections::{HashMap, HashSet};
use crate::external::mongo::*;
use millegrilles_common_rust::async_trait::async_trait;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoImpl, MongoDaoTyped};
use millegrilles_common_rust::mongodb::ClientSession;
use millegrilles_common_rust::serde_json::Value;
use millegrilles_common_rust::v3::impls::transaction_service::TransactionServiceImpl;
use millegrilles_common_rust::v3::models::{BatchInsertions, ErrorMessage, TransactionOperationAggregator, TransactionWrapper};
use millegrilles_common_rust::v3::{ConfigService, FormatService, TransactionRouter, TransactionService};
use std::sync::Arc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::{bson, mongodb};
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageMilleGrillesOwned;
use millegrilles_common_rust::mongodb::options::{InsertOneModel, UpdateOneModel, WriteModel};
use millegrilles_common_rust::tokio_stream::StreamExt;
use crate::constants::DOMAIN_NAME;
use crate::external::mongo_struct::{MediaOwnedRow, NodeFichierRepOwned, NodeFichierVersionOwned};
use crate::external::mq::*;
use crate::flow::directories::get_path_cuuid;
use crate::flow::transaction_models::{get_flags_media, TransactionNouvelleCollection, TransactionNouvelleVersion, TransactionAssocierConversions};
use crate::models::TypeNode;

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
            TRANSACTION_NOUVELLE_COLLECTION => new_collection(self.mongo.as_ref(), wrapper, true).await,
            TRANSACTION_NOUVELLE_COLLECTION_V2 => new_collection(self.mongo.as_ref(), wrapper, false).await,
            TRANSACTION_NOUVELLE_VERSION => new_file_version(self.mongo.as_ref(), wrapper, true).await,
            TRANSACTION_NOUVELLE_VERSION_V2 => new_file_version(self.mongo.as_ref(), wrapper, false).await,
            TRANSACTION_ASSOCIER_CONVERSIONS => associate_file_convertions(self.mongo.as_ref(), wrapper, true).await,
            TRANSACTION_ASSOCIER_CONVERSIONS_V2 => associate_file_convertions(self.mongo.as_ref(), wrapper, false).await,

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

async fn associate_file_convertions<M>(
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
            set_ops.insert("audio", bson::serialize_to_document(audio)?);
        }
        if let Some(subtitles) = transaction.subtitles.as_ref() {
            set_ops.insert("subtitles", bson::serialize_to_document(subtitles)?);
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

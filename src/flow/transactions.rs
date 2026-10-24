use crate::external::mongo::*;
use millegrilles_common_rust::async_trait::async_trait;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::MongoDao;
use millegrilles_common_rust::mongodb::ClientSession;
use millegrilles_common_rust::serde_json::Value;
use millegrilles_common_rust::v3::impls::transaction_service::TransactionServiceImpl;
use millegrilles_common_rust::v3::models::{BatchInsertions, TransactionOperationAggregator, TransactionWrapper};
use millegrilles_common_rust::v3::{ConfigService, FormatService, TransactionRouter, TransactionService};
use std::sync::Arc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::chrono::Utc;
use millegrilles_common_rust::{bson, mongodb};
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageMilleGrillesOwned;
use millegrilles_common_rust::mongodb::options::{InsertOneModel, WriteModel};
use crate::constants::DOMAIN_NAME;
use crate::external::mongo_struct::NodeFichierRepOwned;
use crate::external::mq::*;
use crate::flow::directories::get_path_cuuid;
use crate::flow::transaction_models::TransactionNouvelleCollection;
use crate::models::TypeNode;

pub struct FileManagerTransactionService {
    pub transaction: Arc<dyn TransactionService>,
}

impl FileManagerTransactionService {
    pub fn new(
        config: Arc<dyn ConfigService>,
        format: Arc<dyn FormatService>,
        mongo: Arc<dyn MongoDao>,
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
    mongo: Arc<dyn MongoDao>,
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
            TRANSACTION_NOUVELLE_COLLECTION => true,
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
        flag_rag: Some(false),
        mimetype: None,
        fuuids_versions: None,
        path_cuuids,
        derniere_modification: Some(Utc::now()),
        date_creation: Some(transaction_date.to_owned()),
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

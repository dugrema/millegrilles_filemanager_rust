use crate::external::mongo::*;
use millegrilles_common_rust::async_trait::async_trait;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::MongoDao;
use millegrilles_common_rust::mongodb::ClientSession;
use millegrilles_common_rust::serde_json::Value;
use millegrilles_common_rust::v3::impls::transaction_service::TransactionServiceImpl;
use millegrilles_common_rust::v3::models::{TransactionOperationAggregator, TransactionWrapper};
use millegrilles_common_rust::v3::{ConfigService, FormatService, TransactionRouter, TransactionService};
use std::sync::Arc;

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
            // TRANSACTION_INSCRIRE_USAGER => register_user(self.mongo.as_ref(), wrapper).await,

            _ => Err(CommonError::Str("Unknown transaction action"))
        }
    }
}

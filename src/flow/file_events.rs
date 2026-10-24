use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::chrono::{DateTime, Duration, Utc};
use millegrilles_common_rust::constantes::Securite;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageMilleGrillesOwned;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::{bson, serde_json, tokio};
use millegrilles_common_rust::tokio::select;
use millegrilles_common_rust::tokio_util::sync::CancellationToken;
use millegrilles_common_rust::tracing::{debug, error, info};
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use serde::{Deserialize, Serialize};
use crate::constants::*;
use crate::external::mongo::*;
use crate::external::mongo_struct::NodeFichierRepOwned;
use crate::external::mq::*;
use crate::flow::request_models::ResponseFichierRepVersion;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CollectionEventAggregator {
    /// tuuid of the collection
    cuuid: String,
    // Aggregated events
    #[serde(skip_serializing_if = "Option::is_none")]
    fichiers_ajoutes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fichiers_modifies: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collections_ajoutees: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collections_modifiees: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retires: Option<Vec<String>>,
}

impl CollectionEventAggregator {
    pub fn new(cuuid: &str) -> Self {
        Self {
            cuuid: cuuid.to_string(),
            fichiers_ajoutes: None,
            fichiers_modifies: None,
            collections_ajoutees: None,
            collections_modifiees: None,
            retires: None,
        }
    }

    pub fn fichiers_ajoutes(mut self, value: Vec<String>) -> Self {
        self.fichiers_ajoutes = Some(value);
        self
    }

    pub fn fichiers_modifies(mut self, value: Vec<String>) -> Self {
        self.fichiers_modifies = Some(value);
        self
    }

    pub fn collections_ajoutees(mut self, value: Vec<String>) -> Self {
        self.collections_ajoutees = Some(value);
        self
    }

    pub fn collections_modifiees(mut self, value: Vec<String>) -> Self {
        self.collections_modifiees = Some(value);
        self
    }

    pub fn retires(mut self, value: Vec<String>) -> Self {
        self.retires = Some(value);
        self
    }

    /// Combine deux instances de EvenementContenuCollection
    pub fn merge(&mut self, other: Self) -> Result<(), CommonError> {
        if self.cuuid.as_str() != other.cuuid.as_str() {
            Err(format!("EvenementContenuCollection.merge cuuid mismatch"))?
        }

        match self.fichiers_ajoutes.as_mut() {
            Some(inner) => match other.fichiers_ajoutes {
                Some(inner_other) => inner.extend(inner_other),
                None => () // Rien a faire
            },
            None => self.fichiers_ajoutes = other.fichiers_ajoutes
        }

        match self.fichiers_modifies.as_mut() {
            Some(inner) => match other.fichiers_modifies {
                Some(inner_other) => inner.extend(inner_other),
                None => () // Rien a faire
            },
            None => self.fichiers_modifies = other.fichiers_modifies
        }

        match self.collections_ajoutees.as_mut() {
            Some(inner) => match other.collections_ajoutees {
                Some(inner_other) => inner.extend(inner_other),
                None => () // Rien a faire
            },
            None => self.collections_ajoutees = other.collections_ajoutees
        }

        match self.collections_modifiees.as_mut() {
            Some(inner) => match other.collections_modifiees {
                Some(inner_other) => inner.extend(inner_other),
                None => () // Rien a faire
            },
            None => self.collections_modifiees = other.collections_modifiees
        }

        match self.retires.as_mut() {
            Some(inner) => match other.retires {
                Some(inner_other) => inner.extend(inner_other),
                None => () // Rien a faire
            },
            None => self.retires = other.retires
        }

        Ok(())
    }
}

struct DelayedMessages {
    cuuid: String,
    /// Moment when this batch can be sent
    expiration: DateTime<Utc>,
    message: Option<CollectionEventAggregator>,
}

/// Service that throttles/collapses update events emitted for tuuids (directories, files)
pub struct DelayedEventEmitterService {
    outbound: Arc<MessageOutboundFacade>,
    /// Delay to apply to batch of events being emitted, should be 3 seconds or less.
    delay: Duration,
    /// Max number of messages in buffer
    max_message_count: u16,
    /// Buffer of message to send, key=cuuid/user_id
    cuuids_buffer: Mutex<HashMap<String, DelayedMessages>>,
}

impl DelayedEventEmitterService {
    pub fn new(outbound: Arc<MessageOutboundFacade>, delay: Duration, max_message_count: u16) -> Self {
        Self { outbound, delay, max_message_count, cuuids_buffer: Mutex::new(HashMap::new()) }
    }

    pub async fn run(&self, shutdown_token: CancellationToken) {
        // Start the maintenance thread - it waits on tokio::sleep so force cancel it when stopping.
        select! {
            _ = self.thread() => {},
            _ = shutdown_token.cancelled() => {},
        }
    }

    pub async fn emit(
        &self,
        message: CollectionEventAggregator,
    ) -> Result<(), CommonError> {
        let cuuid = message.cuuid.clone();
        {
            let mut guard = self.cuuids_buffer.lock().expect("Mutex poisoned");
            match guard.get_mut(&cuuid) {
                Some(delayed) => {
                    debug!("Throttling message for {}", cuuid);
                    match delayed.message.as_mut() {
                        Some(buffer) => buffer.merge(message)?,
                        None => {
                            delayed.message = Some(message);
                        }
                    }
                    return Ok(())  // Message is throttled
                },
                None => {
                    if guard.len() < self.max_message_count as usize {
                        // Placeholder used to throttle subsequent messages
                        let delayed = DelayedMessages {
                            cuuid: cuuid.clone(),
                            expiration: Utc::now() + self.delay,
                            message: None,
                        };
                        guard.insert(cuuid, delayed);
                    }

                    // Message is not throttled
                }
            }
        }

        // Emit message immediately
        self.emit_message(message).await?;

        Ok(())
    }

    async fn thread(&self) {
        let duration: tokio::time::Duration = self.delay.to_std().unwrap().into();
        info!("DelayedEventEmitterService thread started with delay of {:?}", duration);
        loop {
            if let Err(e) = self.process_throttled_messages().await {
                error!("Error emitting user messages: {:?}", e);
            }
            tokio::time::sleep(duration.clone()).await;
        }
    }

    async fn process_throttled_messages(&self) -> Result<(), CommonError> {
        let mut cuuids = Vec::with_capacity(self.max_message_count as usize);
        {
            let now = Utc::now();
            let mut guard = self.cuuids_buffer.lock().expect("Mutex poisoned");
            if guard.is_empty() {
                return Ok(());  // Nothing to do
            }

            // Select messages to send
            for (k,v) in guard.iter() {
                if v.expiration < now {
                    cuuids.push(k.clone());
                }
            }
        }

        // Send all retrieved delayed messages
        for cuuid in cuuids {
            self.emit_throttled_message(cuuid.as_str()).await?;
        }

        Ok(())
    }

    async fn emit_throttled_message(&self, cuuid: &str) -> Result<(), CommonError> {
        // Remove message holder from buffer
        let delayed_message = {
            let mut guard = self.cuuids_buffer.lock().expect("Mutex poisoned");
            guard.remove(&cuuid.to_string())
        };

        if let Some(delayed_message) = delayed_message {
            debug!("Releasing throttled message for {}", cuuid);
            if let Some(message) = delayed_message.message {
                self.emit_message(message).await?;
            }
        }

        Ok(())
    }

    async fn emit_message(&self, message: CollectionEventAggregator) -> Result<(), CommonError> {
        let routing = RoutageMessageAction::builder(
            DOMAIN_NAME,
            EVENEMENT_MAJ_CONTENU_COLLECTION,
            vec![Securite::L2Prive]
        )
            .partition(&message.cuuid)
            .build();
        debug!("Emit event message on cuuid {}", message.cuuid);
        self.outbound.emit_event(routing, message).await
    }
}

pub async fn emettre_evenement_maj_fichier<M>(
    mongo: &M,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    tuuid: &str,
) -> Result<(), CommonError> where M: MongoDaoTyped {
    debug!("grosfichiers.emettre_evenement_maj_fichier Emettre evenement maj pour fichier {}", tuuid);

    // Charger fichier
    let filtre = doc! {CHAMP_TUUID: tuuid};
    let collection = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let doc_fichier = match collection.find_one(filtre).await? {
        Some(doc_fichier) => doc_fichier,
        None => return Err(CommonError::String(format!("grosfichiers.emettre_evenement_maj_fichier Document {} introuvable", tuuid)))
    };

    if let Some(cuuids) = doc_fichier.path_cuuids.as_ref() {
        if let Some(cuuid) = cuuids.first() {
            let evenement = CollectionEventAggregator::new(cuuid.as_str())
                .fichiers_modifies(vec![tuuid.to_owned()]);
            delayed_event_emitter_service.emit(evenement).await?;
        }
    }

    Ok(())
}

pub async fn emettre_evenement_maj_collection(
    mongo: &dyn MongoDao,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    tuuid: &str,
    user_id: &str
) -> Result<(), CommonError> {
    debug!("grosfichiers.emettre_evenement_maj_collection Emettre evenement maj pour collection {}", tuuid);

    // Charger fichier
    let filtre = doc! { CHAMP_TUUID: tuuid };
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let file_response: ResponseFichierRepVersion = match collection.find_one(filtre).await {
        Ok(Some(inner)) => bson::deserialize_from_document::<NodeFichierRepOwned>(inner)?.into(),
        Ok(None) => Err(format!("grosfichiers.emettre_evenement_maj_collection Collection {} introuvable", tuuid))?,
        Err(e) => Err(format!("grosfichiers.where Erreur collection.find_one pour {} : {:?}", tuuid, e))?
    };

    debug!("Emitting event majCollection on {} with\n{}", tuuid, serde_json::to_string(&file_response)?);
    let routage = RoutageMessageAction::builder(
        DOMAIN_NAME,
        EVENEMENT_MAJ_COLLECTION,
        vec![Securite::L2Prive]
    )
        .partition(tuuid)
        .build();
    outbound.emit_event(routage, &file_response).await?;

    if let Some(path_cuuids) = file_response.path_cuuids {
        let cuuid_value = match path_cuuids.first() {
            Some(cuuid) => cuuid.as_str(),
            None => user_id
        };
        // Emettre evenement de mise a jour de la collection parent.
        let evenement_modif = CollectionEventAggregator::new(cuuid_value)
            .collections_modifiees(vec![tuuid.to_string()]);
        delayed_event_emitter_service.emit(evenement_modif).await?;
    }

    Ok(())
}

pub async fn update_file_versions_quota(mongo: &dyn MongoDao, user_id: &str, file_size: i64) -> Result<(), CommonError> {
    // Update/insert the user quota information
    let collection_quotas = mongo.get_collection(NOM_COLLECTION_QUOTAS_USAGERS)?;
    let filtre = doc!{ FIELD_USER_ID: user_id };
    let ops = doc!{
        "$setOnInsert": { FIELD_DATE_CREATED: bson::datetime::DateTime::now() },
        "$inc": { "bytes_total_versions": file_size, "nombre_total_versions": 1 },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };

    collection_quotas
        .update_one(filtre, ops)
        .upsert(true)
        .await?;

    Ok(())
}

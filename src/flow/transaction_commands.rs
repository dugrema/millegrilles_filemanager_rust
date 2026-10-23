use crate::constants::*;
use crate::external::mq::*;
use crate::flow::directories::get_path_cuuid;
use crate::flow::file_events::{CollectionEventAggregator, DelayedEventEmitterService, emettre_evenement_maj_collection};
use crate::flow::keys::relay_attached_key;
use crate::flow::transaction_models::TransactionNouvelleCollection;
use crate::flow::transactions::FileManagerTransactionService;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::jwt_simple::prelude::Deserialize;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::serde_json;
use millegrilles_common_rust::tracing::info;
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::v3::{ConfigService, DocumentEncryptionService, MessagingService};
use serde::Serialize;

/// Process the command part of the transaction (checks, validations, volatile updates),
/// calls transaction processor and then handles responses and emits events.
pub async fn process_transaction<M,E>(
    mongo: &M,
    messaging: &dyn MessagingService,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    encryption: &E,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped, E: DocumentEncryptionService
{
    let action = match wrapper.get_routing_action() {
        Some(action) => action,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err("No action provided in transaction")).await
    };

    match wrapper.message.kind {
        MessageKind::Commande => {
            match action {
                TRANSACTION_NOUVELLE_COLLECTION => new_collection(mongo, messaging, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                _ => {
                    info!("Unknown action {} for process_transaction, skipping", action);
                    Ok(())
                }
            }
        },
        _ => {
            info!("Unhandled message type with action {} for process_command, skipping", action);
            Ok(())
        }
    }
}

#[derive(Serialize)]
struct ResponseNewCollection {
    ok: bool,
    tuuid: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandNewCollection {
    pub metadata: DataChiffre,
    pub cuuid: Option<String>,  // Insertion dans collection destination
    pub favoris: Option<bool>,
}

async fn new_collection(
    mongo: &dyn MongoDao,
    messaging: &dyn MessagingService,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    mut wrapper: MessageValidated
) -> Result<(), CommonError> {
    let attachments = wrapper.message.attachements.take();

    // Validate message structure
    let command: CommandNewCollection = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

    // Save the new key with the keymaster
    match attachments {
        Some(mut attachements) => match attachements.remove("cle") {
            Some(cle) => relay_attached_key(messaging, serde_json::from_value(cle)?).await?,
            None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "New collection key is missing")).await
        },
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "New collection key is missing")).await
    }

    let tuuid = wrapper.message.id.clone();
    // let delivery_info = wrapper.delivery_info.clone();

    // Ajouter collection parent au besoin
    let path_cuuids: Option<Vec<String>> = match command.cuuid.as_ref() {
        Some(cuuid) => {
            // Charger le cuuid pour ajouter path vers root
            match get_path_cuuid(mongo, cuuid).await? {
                Some(inner) => Some(inner),
                None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "Unknown directory (cuuid not found)")).await
            }
        },
        None => None
    };

    let transaction_value = TransactionNouvelleCollection {
        metadata: command.metadata,
        cuuid: command.cuuid.clone(),
        favoris: command.favoris,
        cuuids: path_cuuids,
        user_id: Some(user_id.clone()),
    };

    // The unique id of the transaction is the new tuuid
    let tuuid = transaction.process_value(
        DOMAIN_NAME,
        TRANSACTION_NOUVELLE_COLLECTION,
        serde_json::to_value(transaction_value)?,
        None
    ).await?;

    // Emettre fichier pour que tous les clients recoivent la mise a jour
    emettre_evenement_maj_collection(
        mongo,
        outbound,
        delayed_event_emitter_service,
        tuuid.as_str(),
        user_id.as_str()
    ).await?;

    let evenement_contenu = match command.cuuid.as_ref() {
        Some(cuuid) => CollectionEventAggregator::new(cuuid),
        None => CollectionEventAggregator::new(user_id.as_str())
    }
        .collections_ajoutees(vec![tuuid.clone()]);
    delayed_event_emitter_service.emit(evenement_contenu).await?;

    outbound.respond(wrapper.delivery_info, ResponseNewCollection { ok: true, tuuid } ).await
}

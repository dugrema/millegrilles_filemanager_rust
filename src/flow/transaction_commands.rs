use std::collections::HashSet;
use millegrilles_common_rust::bson::doc;
use crate::constants::*;
use crate::external::mq::*;
use crate::flow::directories::get_path_cuuid;
use crate::flow::file_events::{emettre_evenement_maj_collection, emettre_evenement_maj_fichier, CollectionEventAggregator, DelayedEventEmitterService, touch_fichiers_rep, emit_update_events_by_fuuids};
use crate::flow::keys::relay_attached_key;
use crate::flow::transaction_models::*;
use crate::flow::transactions::FileManagerTransactionService;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::constantes::{Securite, ROLE_MEDIA};
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::jwt_simple::prelude::Deserialize;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::serde_json;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{error, info, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::v3::{ConfigService, DocumentEncryptionService, MessagingService};
use serde::Serialize;
use crate::external::mongo::*;
use crate::external::mongo_struct::NodeFichierRepOwned;
use crate::flow::file_jobs::{remove_lease, set_flag_image_traitee};
use crate::flow::maintenance::verifier_visites_topologies;

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
                TRANSACTION_NOUVELLE_VERSION => new_file_version(mongo, messaging, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_ASSOCIER_CONVERSIONS => associate_file_conversions(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,

                _ => {
                    warn!("Unknown action {} for process_transaction, skipping", action);
                    Ok(())
                }
            }
        },
        _ => {
            warn!("Unhandled message type with action {} for process_command, skipping", action);
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

    // Add parent collection if required - may fail if parent is unknown
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

    // Save the new key with the keymaster
    match attachments {
        Some(mut attachements) => match attachements.remove("cle") {
            Some(cle) => relay_attached_key(messaging, serde_json::from_value(cle)?).await?,
            None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "New collection key is missing")).await
        },
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "New collection key is missing")).await
    }

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
        TRANSACTION_NOUVELLE_COLLECTION_V2,
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

async fn new_file_version<M>(
    mongo: &M,
    messaging: &dyn MessagingService,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    mut wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let attachments = wrapper.message.attachements.take();
    let mut command: TransactionNouvelleVersion = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

    // Verify that this file DOES NOT EXIST (by tuuid/fuuid)
    if let Some(tuuid) = command.tuuid.as_ref() {
        let collection_files = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
        let filter = doc! { FIELD_TUUID: tuuid };
        if collection_files.find_one(filter).await?.is_some() {
            return Err(CommonError::Str("File already exists (by tuuid)"))
        }
    }
    let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let filter = doc!{ FIELD_FUUID: &command.fuuid };
    if collection_versions.find_one(filter).await?.is_some() {
        return Err(CommonError::Str("File already exists (by fuuid)"))
    }

    // Add parent collection if required - may fail if parent is unknown
    let path_cuuids: Vec<String> = {
        // Charger le cuuid pour ajouter path vers root
        match get_path_cuuid(mongo, command.cuuid.as_str()).await? {
            Some(inner) => inner,
            None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "Unknown directory (cuuid not found)")).await
        }
    };

    // Update the command with path_cuuids and add the verified user_id.
    // The transaction will be re-signed with the domain's certificate.
    command.path_cuuids = Some(path_cuuids);
    command.user_id = Some(user_id.clone());

    // Save the new key with the keymaster
    match attachments {
        Some(mut attachements) => match attachements.remove("cle") {
            Some(cle) => relay_attached_key(messaging, serde_json::from_value(cle)?).await?,
            None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "New collection key is missing")).await
        },
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "New collection key is missing")).await
    }

    // The unique id of the transaction is the new tuuid
    let new_id = transaction.process_value(
        DOMAIN_NAME,
        TRANSACTION_NOUVELLE_VERSION_V2,
        serde_json::to_value(&command)?,
        None
    ).await?;

    // Determine the new tuuid based on the command
    let tuuid = command.tuuid.unwrap_or_else(|| new_id);
    if let Err(e) = emettre_evenement_maj_fichier(mongo, delayed_event_emitter_service, tuuid.as_str()).await {
        warn!("Error sending out file/collection update event: {:?}", e);
    };

    // Emit file claim - allows it to be synchronized to all filehosts immediately
    if let Err(e) = verifier_visites_topologies(outbound, &vec![command.fuuid.as_str()]).await {
        warn!("Error claiming new file version {}: {:?}", tuuid, e);
    }

    outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await
}

async fn associate_file_conversions<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let mut command: TransactionAssocierConversions = wrapper.message.deserialize()?;

    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_MEDIA.to_string()])? ||
        ! wrapper.certificate.verifier_exchanges(vec![Securite::L2Prive])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    };

    // Make a list of affected user_ids. The transaction will be completed with this information.
    {
        let filtre_reps = doc! { FIELD_FUUIDS_VERSIONS: &command.fuuid };
        let collection_reps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
        let mut user_ids = HashSet::with_capacity(20);
        let mut cursor = collection_reps.find(filtre_reps).await?;
        while let Some(row) = cursor.next().await {
            user_ids.insert(row?.user_id);
        }
        if user_ids.is_empty() {
            return Err(CommonError::String(format!("transaction_associer_conversions No match for fuuid {}", command.fuuid)))
        }
        command.user_ids = Some(user_ids.into_iter().collect::<Vec<String>>());
    }

    // Sign a new transaction that includes the user_ids.
    transaction.process_value(
        DOMAIN_NAME,
        TRANSACTION_ASSOCIER_CONVERSIONS_V2,
        serde_json::to_value(&command)?,
        None
    ).await?;

    if let Err(e) = touch_fichiers_rep(mongo, &vec![command.fuuid.as_str()]).await {
        error!("commande_associer_conversions Erreur touch_fichiers_rep {} : {:?}", command.fuuid, e);
    }

    if let Err(e) = set_flag_image_traitee(mongo, command.fuuid.as_str()).await {
        error!("transaction_associer_conversions Erreur set flag true pour traitement job images {} : {:?}", command.fuuid, e);
    }

    remove_lease(mongo, command.fuuid.as_str(), CHAMP_FLAG_MEDIA_TRAITE).await?;

    // Emit all tuuids matching this file to ensure listeners get notified
    emit_update_events_by_fuuids(mongo, delayed_event_emitter_service, &vec![command.fuuid.as_str()]).await?;

    // Emit file claim - allows it to be synchronized to all filehosts immediately
    let mut image_fuuids = Vec::new();
    for (_, image) in &command.images {
        if image.data_chiffre.is_none() {
            // This is not an inline thumbnail
            image_fuuids.push(image.hachage.as_str());
        }
    }

    if ! image_fuuids.is_empty() {
        if let Err(e) = verifier_visites_topologies(outbound, &image_fuuids).await {
            warn!("commande_associer_conversions Error claiming images for file {}: {:?}", command.fuuid, e);
        }
    }

    outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await
}

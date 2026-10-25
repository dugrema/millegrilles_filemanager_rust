use std::collections::HashSet;
use millegrilles_common_rust::bson::doc;
use crate::constants::*;
use crate::external::mq::*;
use crate::flow::directories::get_path_cuuid;
use crate::flow::file_events::{emit_rep_update_event, emettre_evenement_maj_fichier, CollectionEventAggregator, DelayedEventEmitterService, touch_fichiers_rep, emit_update_events_by_fuuids};
use crate::flow::keys::relay_attached_key;
use crate::flow::transaction_models::*;
use crate::flow::transactions::FileManagerTransactionService;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::constantes::{Securite, ROLE_MEDIA};
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::jwt_simple::prelude::Deserialize;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::serde_json;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::{ErrorMessage, TransactionOperationAggregator, TransactionWrapper};
use millegrilles_common_rust::v3::{ConfigService, DocumentEncryptionService, MessagingService};
use serde::Serialize;
use crate::external::mongo::*;
use crate::external::mongo_struct::{NodeFichierRepOwned, NodeFichierVersionOwned};
use crate::flow::file_jobs::{remove_lease, set_flag_image_traitee, BackgroundJob, sauvegarder_job_video, trigger_file_indexing, set_flag_video_traite};
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
                // Adding, updating files
                TRANSACTION_NOUVELLE_COLLECTION => new_collection(mongo, messaging, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_NOUVELLE_VERSION => new_file_version(mongo, messaging, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_DECRIRE_FICHIER => update_file_metadata(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_DECRIRE_COLLECTION => update_collection_metadata(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_UPDATE_FILE_TEXT_CONTENT => update_file_text_content(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_DELETE_FILE_COMMENT => delete_file_comment(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_FILE_SUMMARY => todo!(),

                // Moving files
                TRANSACTION_DEPLACER_FICHIERS_COLLECTION => todo!(),
                TRANSACTION_SUPPRIMER_DOCUMENTS => todo!(),
                TRANSACTION_RECUPERER_DOCUMENTS_V2 => todo!(),
                TRANSACTION_RECYCLE_ITEMS_V3 => todo!(),
                TRANSACTION_SUPPRIMER_ORPHELINS => todo!(),
                TRANSACTION_DELETE_V2 => todo!(),
                TRANSACTION_MOVE_V2 => todo!(),
                TRANSACTION_COPY_V2 => todo!(),
                TRANSACTION_PERMANENTLY_DELETE_FILES => todo!(),

                // Media
                TRANSACTION_ASSOCIER_VIDEO => associate_video_conversion(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_ASSOCIER_CONVERSIONS => associate_file_conversions(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_SUPPRIMER_VIDEO => todo!(),
                TRANSACTION_IMAGE_SUPPRIMER_JOB_V2 => todo!(),
                COMMAND_VIDEO_DELETE_JOB_V2 => delete_video_job(mongo, outbound, transaction, wrapper).await,
                TRANSACTION_ADD_WEB_SUBTITLE => todo!(),
                TRANSACTION_REMOVE_WEB_SUBTITLE => todo!(),

                // Sharing
                TRANSACTION_AJOUTER_CONTACT_LOCAL => todo!(),
                TRANSACTION_SUPPRIMER_CONTACTS => todo!(),
                TRANSACTION_PARTAGER_COLLECTIONS => todo!(),
                TRANSACTION_SUPPRIMER_PARTAGE_USAGER => todo!(),

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
            // Check that the user has access (owns) to parent collection
            let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
            let filtre_reps = doc!{ FIELD_TUUID: cuuid, FIELD_USER_ID: &user_id };
            if collection_reps.find_one(filtre_reps).await?.is_none() {
                return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
            }

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
    emit_rep_update_event(
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
        // Check that the user has access (owns) the collection
        let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
        let filtre_reps = doc!{ FIELD_TUUID: tuuid, FIELD_USER_ID: &user_id };
        if collection_reps.find_one(filtre_reps).await?.is_none() {
            return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
        }

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

async fn update_file_metadata<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let command: TransactionDecrireFichier = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

    // Fetch information for triggers once transaction is complete
    // Find matching file
    let collection_reps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre_reps = doc!{ FIELD_TUUID: &command.tuuid, FIELD_USER_ID: &user_id };
    let file_rep = match collection_reps.find_one(filtre_reps).await? {
        Some(inner) => inner,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
    };

    // If this is a file, extract the first fuuid (most recent version)
    let fuuid = match file_rep.fuuids_versions.as_ref() {
        Some(inner) => {
            if let Some(fuuid) = inner.first() {
                fuuid.to_owned()
            } else {
                return Err(CommonError::String(format!("commande_decrire_fichier No file version found for tuuid:{}", command.tuuid)))
            }
        },
        None => return Err(CommonError::String(format!("commande_decrire_fichier No file version found for tuuid:{}", command.tuuid)))
    };

    let changement_media = match command.mimetype.as_ref() {
        Some(mimetype) => {
            debug!("commande_decrire_fichier Verifier si le mimetype du fichier a change (nouveau: {})", mimetype);
            if file_rep.mimetype != command.mimetype {
                debug!("commande_decrire_fichier Le mimetype a change de {:?} vers {:?}, reset traitement media de {}",
                    file_rep.mimetype, command.mimetype, command.tuuid);
                true
            } else {
                false
            }
        },
        None => false
    };

    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    let (_flag_media_traite, flag_video_traite) = if changement_media {
        if let Some(mimetype) = command.mimetype.as_ref() {
            // Ajouter flags media au fichier si approprie
            let (flag_media_traite, flag_video_traite, _, flag_summary) = get_flags_media(
                mimetype.as_str());
            let filtre = doc! {CHAMP_FUUID: &fuuid};
            let ops = doc! {
                "$set": {
                    // CHAMP_FLAG_MEDIA: flag_media,
                    CHAMP_FLAG_MEDIA_TRAITE: flag_media_traite,
                    CHAMP_FLAG_VIDEO_TRAITE: flag_video_traite,
                    CHAMP_FLAG_SUMMARY: flag_summary,
                },
                "$currentDate": { FIELD_DATE_MODIFIED: true }
            };
            debug!("commande_decrire_fichier Reset flags media sur changement mimetype pour {} : {:?}", command.tuuid, ops);
            let collection = mongo.get_collection_typed::<NodeFichierVersionOwned>(
                NOM_COLLECTION_VERSIONS)?;
            collection.update_one(filtre.clone(), ops).await?;
            (flag_media_traite, flag_video_traite)
        } else {
            warn!("commande_decrire_fichier Erreur utilisation fuuid sur changement (None)");
            (true, true)  // prevent media jobs from being created
        }
    } else {
        (true, true)  // prevent media jobs from being created
    };

    let tuuid = &command.tuuid;

    // Declencher indexation
    if ! flag_video_traite {
        let filtre = doc! {"fuuid": &fuuid};
        let collection = mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;
        if let Some(row) = collection.find_one(filtre).await? {
            if row.cle_id.is_some() && row.format.is_some() && row.nonce.is_some() {

                // Create indexing job
                let cle_id = row.cle_id.clone().expect("cle_id");
                let format: &str = row.format.clone().expect("format").into();
                let nonce = row.nonce.clone().expect("nonce");
                let mimetype = row.mimetype.clone();
                let filehost_ids: Vec<&String> = row.visites.keys().collect();
                let job = BackgroundJob::new_index(
                    tuuid,
                    Some(fuuid.as_str()),
                    &user_id,
                    mimetype.as_str(),
                    &filehost_ids.iter().map(|id| id.as_str()).collect::<Vec<&str>>(),
                    cle_id.as_str(),
                    format,
                    nonce.as_str(),
                );

                // Create video job
                sauvegarder_job_video(mongo, outbound, &job).await?;
            }
        }
    }

    // Emettre fichier pour que tous les clients recoivent la mise a jour
    if let Err(e) = emettre_evenement_maj_fichier(mongo, delayed_event_emitter_service, &tuuid).await {
        warn!("transaction_decire_fichier Erreur emettre_evenement_maj_fichier : {:?}", e);
    }

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

async fn update_collection_metadata<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let command: TransactionDecrireCollection = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

    // Check that file exists and the user has access to modify
    let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre_reps = doc!{ FIELD_TUUID: &command.tuuid, FIELD_USER_ID: &user_id };
    if collection_reps.find_one(filtre_reps).await?.is_none() {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
    }

    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    // Emettre fichier pour que tous les clients recoivent la mise a jour
    emit_rep_update_event(mongo, outbound, delayed_event_emitter_service, command.tuuid.as_str(), user_id.as_str()).await?;

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

async fn update_file_text_content<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: TransactionUpdateFileTextContent = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

    // Check that file exists and the user has access to modify
    let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre_reps = doc!{ FIELD_TUUID: &command.tuuid, FIELD_USER_ID: &user_id };
    if collection_reps.find_one(filtre_reps).await?.is_none() {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
    }

    // Process transaction
    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    // Emit file modified event
    emit_rep_update_event(mongo, outbound, delayed_event_emitter_service, command.tuuid.as_str(), user_id.as_str()).await?;

    if let Err(e) = trigger_file_indexing(outbound).await {
        warn!("update_file_text_content Error emitting file indexing trigger: {:?}", e);
    }

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

async fn delete_file_comment<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let command: TransactionDeleteFileComment = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

    // Check that file exists and the user has access to modify
    let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre_reps = doc!{ FIELD_TUUID: &command.tuuid, FIELD_USER_ID: &user_id };
    if collection_reps.find_one(filtre_reps).await?.is_none() {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
    }

    // Process transaction
    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    // Emit file modified event
    emit_rep_update_event(mongo, outbound, delayed_event_emitter_service, command.tuuid.as_str(), user_id.as_str()).await?;

    if let Err(e) = trigger_file_indexing(outbound).await {
        warn!("update_file_text_content Error emitting file indexing trigger: {:?}", e);
    }

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

async fn associate_video_conversion<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let command: TransactionAssocierVideo = wrapper.message.deserialize()?;

    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_MEDIA.to_string()])? ||
        ! wrapper.certificate.verifier_exchanges(vec![Securite::L2Prive])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    };

    // Check that file exists and the user has access to modify
    let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre_reps = doc!{ FIELD_TUUID: &command.tuuid, FIELD_USER_ID: &command.user_id };
    if collection_reps.find_one(filtre_reps).await?.is_none() {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
    }

    // Process transaction
    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    // Remove video job
    let job_id = match command.job_id.as_ref() {Some(inner)=>Some(inner.as_str()), None=>None};
    let tuuid = match command.tuuid.as_ref() {Some(inner)=>Some(inner.as_str()), None=>None};
    set_flag_video_traite(mongo, tuuid, command.fuuid.as_str(), job_id).await?;

    // Touch - s'assure que le client va voir que le fichier a ete modifie (sync)
    if let Err(e) = touch_fichiers_rep(mongo, &vec![command.fuuid.as_str()]).await {
        error!("commande_associer_video Erreur touch_fichiers_rep {:?}/{:?} : {:?}", command.user_id, command.fuuid, e);
    }

    // Emettre fichier pour que tous les clients recoivent la mise a jour
    if let Some(tuuid) = command.tuuid.as_ref() {
        if let Err(e) = emit_rep_update_event(
            mongo,
            outbound,
            delayed_event_emitter_service,
            tuuid.as_str(),
            command.user_id.as_str()
        ).await {
            warn!("commande_associer_video Erreur emettre_evenement_maj_fichier : {:?}", e);
        }
    }

    // Emit file claim - allows it to be synchronized to all filehosts immediately
    if let Err(e) = verifier_visites_topologies(outbound, &vec![command.fuuid_video.as_str()]).await {
        warn!("commande_associer_conversions Error claiming new video for file {:?}: {:?}", command.tuuid, e);
    }

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

async fn delete_video_job<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let command: TransactionSupprimerJobVideoV2 = wrapper.message.deserialize()?;

    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if ! wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    };

    {
        // Re-emit the command as event to cancel execution
        let routage = RoutageMessageAction::builder(
            DOMAIN_NAME, EVENEMENT_ANNULER_JOB_VIDEO, vec![Securite::L3Protege])
            .build();
        outbound.emit_event(routage, &command).await?;
    }

    // Determiner si la job est active et pour le video initial (params.defaults == 'true')
    let collection = mongo.get_collection_typed::<BackgroundJob>(NOM_COLLECTION_VIDEO_JOBS)?;
    let filtre = doc!{ "job_id": &command.job_id, FIELD_USER_ID: &user_id };
    let delivery_info = wrapper.delivery_info.clone();
    if let Some(job) = collection.find_one(filtre).await? {
        let initial = match job.params {
            Some(inner) => inner.defaults.unwrap_or(false),
            None => false
        };

        if initial {
            debug!("commande_supprimer_job_video_v2 Error during initial video processing, abort for good on fuuid {}", command.fuuid);
            // Convert into transaction, this ensures the initial video creation does not restart on DB restoration
            transaction.process_transaction(wrapper.into(), None).await?;
        }

        // Emettre un evenement pour clients
        if let Some(user_id) = job.user_id.as_ref() {
            // let evenement = json!({"job_id": &commande.job_id, "fuuid": &commande.fuuid, "tuuid": &commande.tuuid});
            let routage = RoutageMessageAction::builder(
                DOMAIN_NAME,
                "jobSupprimee",
                vec![Securite::L2Prive]
            )
                .partition(user_id)
                .build();
            outbound.emit_event(routage, &command).await?;
        }
    }

    debug!("Delete video job {}", command.job_id);
    set_flag_video_traite(mongo, Some(&command.tuuid), &command.fuuid, Some(&command.job_id)).await?;

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

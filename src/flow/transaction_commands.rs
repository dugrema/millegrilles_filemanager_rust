use crate::constants::*;
use crate::external::mongo::*;
use crate::external::mongo_struct::{MediaOwnedRow, NodeFichierRepOwned, NodeFichierVersionOwned};
use crate::external::mq::*;
use crate::flow::access::{verifier_acces_usager_media, verify_user_access_tuuids, verify_user_certificate};
use crate::flow::directories::{claim_files_by_deleted_tuuids, claim_files_by_fuuids, claim_files_under_deleted_cuuids, get_complete_files, get_path_cuuid, parse_selection_directories};
use crate::flow::file_events::*;
use crate::flow::file_jobs::{BackgroundJob, remove_lease, sauvegarder_job_video, set_flag_image_traitee, set_flag_video_traite, trigger_file_indexing};
use crate::flow::keys::relay_attached_key;
use crate::flow::maintenance::verifier_visites_topologies;
use crate::flow::transaction_models::*;
use crate::flow::transactions::FileManagerTransactionService;
use crate::models::TypeNode;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::constantes::{DOMAINE_NOM_MAITREDESCOMPTES, ROLE_MEDIA, ROLE_OLLAMA_RELAI, RolesCertificats, Securite};
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::jwt_simple::prelude::Deserialize;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::{bson, hex, serde_json};
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::v3::MessagingService;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use crate::flow::file_sharing::get_shared_cuuids_by_userid;

/// Process the command part of the transaction (checks, validations, volatile updates),
/// calls transaction processor and then handles responses and emits events.
pub async fn process_transaction<M>(
    mongo: &M,
    messaging: &dyn MessagingService,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
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
                TRANSACTION_FILE_SUMMARY => file_summary(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,

                // Moving files
                TRANSACTION_AJOUTER_FICHIERS_COLLECTION => copy_files_collections(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_DEPLACER_FICHIERS_COLLECTION => move_files_collections(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_SUPPRIMER_DOCUMENTS => delete_file_reps(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_RECYCLE_ITEMS_V3 | TRANSACTION_RECYCLE_ITEMS_V4 => recycle_items(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,

                // Media
                TRANSACTION_ASSOCIER_VIDEO => associate_video_conversion(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_ASSOCIER_CONVERSIONS => associate_file_conversions(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_SUPPRIMER_VIDEO => delete_video(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_IMAGE_SUPPRIMER_JOB_V2 => delete_image_job(mongo, outbound, transaction, wrapper).await,
                COMMAND_VIDEO_DELETE_JOB_V2 => delete_video_job(mongo, outbound, transaction, wrapper).await,
                TRANSACTION_ADD_WEB_SUBTITLE => add_web_subtitle(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,
                TRANSACTION_REMOVE_WEB_SUBTITLE => remove_web_subtitle(mongo, outbound, delayed_event_emitter_service, transaction, wrapper).await,

                // Sharing
                TRANSACTION_AJOUTER_CONTACT_LOCAL => share_add_contact(mongo, outbound, transaction, wrapper).await,
                TRANSACTION_SUPPRIMER_CONTACTS => share_remove_contact(mongo, outbound, transaction, wrapper).await,
                TRANSACTION_PARTAGER_COLLECTIONS => share_add_collection(mongo, outbound, transaction, wrapper).await,
                TRANSACTION_SUPPRIMER_PARTAGE_USAGER => share_remove_collection(mongo, outbound, transaction, wrapper).await,

                // Obsolete?
                TRANSACTION_RECUPERER_DOCUMENTS_V2 => obsolete(outbound, wrapper).await,

                _ => unknown(outbound, wrapper).await
            }
        },
        _ => unknown(outbound, wrapper).await
    }
}

async fn obsolete(outbound: &MessageOutboundFacade, wrapper: MessageValidated) -> Result<(), CommonError> {
    warn!("Obsolete transaction command called: {}", wrapper.delivery_info.routing_key);
    outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(410, "Obsolete")).await
}

async fn unknown(outbound: &MessageOutboundFacade, wrapper: MessageValidated) -> Result<(), CommonError> {
    warn!("Unknown transaction command called: {}", wrapper.delivery_info.routing_key);
    outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "Unknown command")).await
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

    // Check that the user has access (owns) the collection
    let collection_reps = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre_reps = doc!{ FIELD_TUUID: &command.cuuid, FIELD_USER_ID: &user_id };
    if collection_reps.find_one(filtre_reps).await?.is_none() {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "Directory not found")).await
    }

    // Verify that this file DOES NOT EXIST (by tuuid/fuuid)
    if let Some(tuuid) = command.tuuid.as_ref() {
        let collection_files = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
        let filter = doc! { FIELD_TUUID: tuuid };
        if collection_files.find_one(filter).await?.is_some() {
            return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(409, "File already exists")).await
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

#[derive(Clone, Deserialize)]
struct CommandeSupprimerV2 {
    tuuids: Vec<String>,
}

#[derive(Serialize)]
struct CommandeSupprimerTuuidsIndex {
    tuuids: Vec<String>
}

async fn delete_file_reps<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: CommandeSupprimerV2 = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if !wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

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

    // This command may span multiple transactions depending on the number of rows affected (sub-directories and all files)
    let mut session = mongo.get_session().await?;
    session.start_transaction().await?;

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
            transaction.process_value(
                DOMAIN_NAME,
                TRANSACTION_DELETE_V3,
                serde_json::to_value(transaction_value)?,
                Some(&mut session)
            ).await?;
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

    transaction.process_value(
        DOMAIN_NAME,
        TRANSACTION_DELETE_V3,
        serde_json::to_value(transaction_value)?,
        Some(&mut session)
    ).await?;

    // Operations processed, commit session
    session.commit_transaction().await?;

    debug!("commande_supprimer_documents Post transaction cleanup for : {:?}", tuuids_by_cuuid);

    // Emettre evenements supprime par cuuid
    for (cuuid, liste) in tuuids_by_cuuid.into_iter() {
        let evenement = CollectionEventAggregator::new(cuuid.as_str())
            .retires(liste.clone());
        delayed_event_emitter_service.emit(evenement).await?;

        // Cleanup of the search index. The list may be large and take a while to process.
        // Just let it go after a short wait, not a big deal if it fails. It can be cleaned-up by reindexing the DB.
        let routing = RoutageMessageAction::builder("solrrelai", "supprimerTuuids", vec![Securite::L3Protege])
            .timeout_blocking(300)
            .build();
        let commande_index = CommandeSupprimerTuuidsIndex { tuuids: liste };
        match outbound.send_command(routing, &commande_index).await? {
            Some(response) => match response.is_err()? {
                (true, e) => {
                    warn!("Error on command to delete indexing for deleted tuuid:{:?}: {:?}", commande_index.tuuids, e);
                },
                _ => ()
            },
            None => warn!("No response on command to delete indexing for deleted tuuid:{:?}", commande_index.tuuids)
        }
    }

    outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await
}

async fn delete_video<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: TransactionSupprimerVideo = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if !wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

    // Check that file exists and the user has access to modify
    let collection_versions = mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;
    let filter_version = doc!{ CHAMP_FUUIDS_RECLAMES: &command.fuuid_video };
    let version = match collection_versions.find_one(filter_version).await? {
        Some(version) => version,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
    };
    let collection_reps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let filter_rep = doc!{ FIELD_TUUID: { "$in": &version.tuuids }, FIELD_USER_ID: &user_id };
    if collection_reps.find_one(filter_rep).await?.is_none() {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
    }

    // Load media information for original fuuid/user_id
    let filtre_media = doc!{ FIELD_FUUID: &version.fuuid, FIELD_USER_ID: &user_id };
    let collection_media = mongo.get_collection_typed::<MediaOwnedRow>(NOM_COLLECTION_MEDIA)?;
    let media_row = match collection_media.find_one(filtre_media.clone()).await {
        Ok(d) => match d {
            Some(d) => d,
            None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "Video not found")).await
        },
        Err(e) => Err(format!("transaction_supprimer_video Erreur chargement info document : {:?}", e))?
    };
    
    let videos = match media_row.video {
        Some(videos) => videos,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "Video not found")).await
    };
    let mut labels = Vec::with_capacity(3);
    for (label, video) in videos {
        if video.fuuid_video.as_str() == command.fuuid_video.as_str() {
            labels.push(label.clone());
        }
    }
    
    if labels.is_empty() {
        debug!("No matching label found for fuuid_video: {}", command.fuuid_video);
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "Video not found")).await
    }
    
    // Prepare updated transaction
    let transaction_value = TransactionSupprimerVideoV2 {
        fuuid_video: command.fuuid_video.clone(),
        labels,
        fuuid: version.fuuid.clone(),
        user_id: user_id.clone(),
    };

    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_value(
        DOMAIN_NAME, 
        TRANSACTION_SUPPRIMER_VIDEO_V2, 
        serde_json::to_value(transaction_value)?, 
        None
    ).await?;

    if let Err(e) = touch_fichiers_rep(mongo, &vec![version.fuuid.as_str()]).await {
        error!("transaction_favoris_creerpath Erreur touch_fichiers_rep {:?}/{:?} : {:?}", user_id, version.fuuid, e);
    }

    // Emit update events
    for tuuid in version.tuuids {
        if let Err(e) = emettre_evenement_maj_fichier(mongo, delayed_event_emitter_service, tuuid.as_str()).await {
            warn!("commande_supprimer_video Erreur emettre_evenement_maj_fichier : {:?}", e);
        }
    }

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionDeplacerFichiersCollection {
    pub cuuid_origine: String,          // Collection avec les documents (source)
    pub cuuid_destination: String,      // Collection qui recoit les documents
    pub inclure_tuuids: Vec<String>,    // Fichiers/rep a ajouter a la collection
}

async fn move_files_collections<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: TransactionDeplacerFichiersCollection = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match wrapper.certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "No user ID in certificate")).await
    };
    if !wrapper.certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Not a user certificate")).await
    };

    let mut session = mongo.get_session().await?;
    session.start_transaction().await?;

    let result = parse_selection_directories(
        mongo,
        user_id.as_str(),
        &command.cuuid_destination,
        &command.inclure_tuuids,
        true,
        true,
        &mut session
    ).await?;

    // Check that the destination is not under the source (moving under itself breaks the graph)
    if let Some(directory_moves) = result.directories.as_ref() {
        for directory_move in directory_moves {
            for directory in &directory_move.directories {
                if result.destination_path.contains(directory) {
                    debug!("commande_deplacer_fichiers_collection Rejecting move directory, the destination is under the source (this breaks the graph)");
                    return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(409, "Cannot move directory under itself")).await
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

    debug!("commande_deplacer_fichiers_collection Transaction\n{}", serde_json::to_string(&transaction_value)?);

    transaction.process_value(
        DOMAIN_NAME,
        TRANSACTION_MOVE_V2,
        serde_json::to_value(transaction_value)?,
        Some(&mut session)
    ).await?;

    // Job done, commit then emit events (they are transaction agnostic)
    session.commit_transaction().await?;

    for tuuid in &command.inclure_tuuids {
        // Emettre fichier pour que tous les clients recoivent la mise a jour
        if let Err(e) = emettre_evenement_maj_fichier(mongo, delayed_event_emitter_service, &tuuid).await {
            warn!("transaction_deplacer_fichiers_collection Erreur emettre_evenement_maj_fichier : {:?}", e);
        }
    }

    let destination_events = CollectionEventAggregator::new(command.cuuid_destination.as_str())
        .fichiers_ajoutes(command.inclure_tuuids.clone());
    let source_events = CollectionEventAggregator::new(command.cuuid_origine.as_str())
        .retires(command.inclure_tuuids.clone());

    if let Err(e) = delayed_event_emitter_service.emit(source_events).await {
        warn!("move_files_collections Error delayed_event_emitter_service (source events) : {:?}", e);
    }
    if let Err(e) = delayed_event_emitter_service.emit(destination_events).await {
        warn!("move_files_collections Error delayed_event_emitter_service (destination events) : {:?}", e);
    }

    outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionAjouterFichiersCollection {
    pub cuuid: String,                  // Collection qui recoit les documents
    pub inclure_tuuids: Vec<String>,    // Fichiers/rep a ajouter a la collection
    pub contact_id: Option<String>,     // Permission de copie a partir d'un partage
    pub include_deleted: Option<bool>,  // Allows copying from trash to new section
}

const COPY_FILE_BATCH_SIZE: usize = 250;

async fn copy_files_collections<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: TransactionAjouterFichiersCollection = wrapper.message.deserialize()?;

    // Validation of command
    if command.inclure_tuuids.contains(&command.cuuid) {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "Cannot copy a directory on itself")).await
    }

    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(e) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, format!("Access denied: {:?}", e).as_str())).await
    };
    let include_deleted = command.include_deleted.unwrap_or(false);

    let allowed_cuuids = match command.contact_id.as_ref() {
        Some(contact_id) => {
            debug!("request_files_by_tuuid Allow any shared collection for user_id: {}", user_id);
            let allowed_cuuids = get_shared_cuuids_by_userid(
                mongo,
                user_id.as_str(),
                None,
                Some(contact_id)
            ).await?;
            Some(allowed_cuuids)
        },
        None => None
    };

    // The message.id will be used to create new tuuids, using the first 56 hex chars and padding
    // with 8 more using a counter for uniqueness.
    let tuuid_prefix = wrapper.message.id.as_str()[0..56].to_string();
    let mut tuuid_counter: u32 = 0;

    let mut file_tuuids: Vec<String> = Vec::with_capacity(command.inclure_tuuids.len());
    // Make list of all directories, create new hierarchy with updated cuuids. Map by old cuuid.
    // Key is old cuuid, value is new generated cuuid for this entry
    let mut map_new_by_old_cuuid: HashMap<String, String> = HashMap::with_capacity(100);

    let collection_filereps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;

    // Get destination information
    let filter_destination = doc! { FIELD_TUUID: &command.cuuid, FIELD_USER_ID: &user_id };
    let destination_filerep = match collection_filereps.find_one(filter_destination).await? {
        Some(inner) => inner,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "Destination directory not found")).await
    };
    // Create the new destination path that includes the tuuid destination directory.
    let destination_path_cuuid = match destination_filerep.path_cuuids {
        Some(mut cuuids) => {
            cuuids.insert(0, destination_filerep.tuuid.clone());
            cuuids
        },
        None => vec![destination_filerep.tuuid.clone()]
    };

    let mut filter_request = doc! { FIELD_TUUID: { "$in": &command.inclure_tuuids } };
    match allowed_cuuids.as_ref() {
        Some(cuuids) => {
            // Filter on either authenticated user_id (local) or allowed parent folders (cuuids)
            filter_request.insert("$or",  bson::serialize_to_bson(&vec![
                doc!{ FIELD_USER_ID: &user_id },
                doc!{ FIELD_TUUID: { "$in": &cuuids } },
                doc!{ FIELD_PATH_CUUIDS: { "$in": cuuids } },
            ])?);
        },
        None => {
            // Filter on authenticated user_id
            filter_request.insert(FIELD_USER_ID, &user_id);
        }
    }

    let mut cursor = collection_filereps.find(filter_request).await?;
    while let Some(row) = cursor.next().await {
        let file_rep = row?;
        let node_type: TypeNode = file_rep.type_node.as_str().try_into()?;
        match node_type {
            TypeNode::Fichier => file_tuuids.push(file_rep.tuuid),
            TypeNode::Collection | TypeNode::Repertoire => {
                // Generate a new tuuid for this directory
                let new_tuuid = format!("{}{:08x}", tuuid_prefix, tuuid_counter);
                tuuid_counter += 1;
                map_new_by_old_cuuid.insert(file_rep.tuuid, new_tuuid);
            }
        }
    }

    // Further query to get all subdirectories
    let top_level_directories: Vec<&String> = map_new_by_old_cuuid.keys().collect();
    debug!("Top level directories: {:?}", top_level_directories);
    let mut filter_request = doc! {
        FIELD_PATH_CUUIDS: { "$in": top_level_directories },
        FIELD_NODE_TYPE: TypeNode::Repertoire.to_str(),
    };
    if ! include_deleted {
        filter_request.insert(FIELD_DELETED, false);
    }
    match allowed_cuuids.as_ref() {
        Some(cuuids) => {
            // Filter on either authenticated user_id (local) or allowed parent folders (cuuids)
            filter_request.insert("$or",  bson::serialize_to_bson(&vec![
                doc!{ FIELD_USER_ID: &user_id },
                doc!{ FIELD_TUUID: { "$in": &cuuids } },
                doc!{ FIELD_PATH_CUUIDS: { "$in": cuuids } },
            ])?);
        },
        None => {
            // Filter on authenticated user_id
            filter_request.insert(FIELD_USER_ID, &user_id);
        }
    }

    let mut cursor = collection_filereps.find(filter_request).await?;
    while let Some(row) = cursor.next().await {
        let file_rep = row?;
        let node_type: TypeNode = file_rep.type_node.as_str().try_into()?;
        match node_type {
            TypeNode::Repertoire => {
                // Generate a new tuuid for this directory
                let new_tuuid = format!("{}{:08x}", tuuid_prefix, tuuid_counter);
                tuuid_counter += 1;
                map_new_by_old_cuuid.insert(file_rep.tuuid, new_tuuid);
            },
            _ => return Err(CommonError::Str("Wrong type of entry (should be TypeNode::Repertoire)")),
        }
    }
    debug!("All directories mapped: {:?}", map_new_by_old_cuuid.keys());

    let mut session = mongo.get_session().await?;
    session.start_transaction().await?;

    let mut command_copy = wrapper.message.clone();
    command_copy.certificat = None;  // Remove certificate, keeping a copy for reference

    let mut batch_counter = 1;
    let mut transaction_value = TransactionCopyV3 {
        command: command_copy.clone(),
        entries: Vec::with_capacity(COPY_FILE_BATCH_SIZE),
        destination: destination_path_cuuid.clone(),
        user_id: user_id.clone(),
        include_deleted,
        batch_no: Some(batch_counter),
    };

    // All impacted directories are listed with a new tuuid. Start processing the transaction by
    // going through all directories listed in the map (keys) and applying their new tuuids/path_cuuids.
    let mut map_new_path_by_old_cuuid: HashMap<String, Vec<String>> = HashMap::new();

    let all_directories: Vec<&String> = map_new_by_old_cuuid.keys().collect();
    let mut filter_directories = doc! {
        FIELD_TUUID: { "$in": &all_directories },
    };
    match allowed_cuuids.as_ref() {
        Some(cuuids) => {
            // Filter on either authenticated user_id (local) or allowed parent folders (cuuids)
            filter_directories.insert("$or",  bson::serialize_to_bson(&vec![
                doc!{ FIELD_USER_ID: &user_id },
                doc!{ FIELD_TUUID: { "$in": &cuuids } },
                doc!{ FIELD_PATH_CUUIDS: { "$in": cuuids } },
            ])?);
        },
        None => {
            // Filter on authenticated user_id
            filter_directories.insert(FIELD_USER_ID, &user_id);
        }
    }
    let directory_entries = get_complete_copy(mongo, filter_directories).await?;
    for mut directory in directory_entries {
        let old_tuuid = &directory.file_rep.tuuid;
        let new_tuuid = match map_new_by_old_cuuid.get(old_tuuid) {
            Some(tuuid) => tuuid,
            None => return Err(CommonError::Str("Mapping error, directory missing"))
        };

        // Map the new path cuuids. Start replacing until a cuuid is not found, then append
        // destination path.
        let new_path_cuuids: Vec<String> = match directory.file_rep.path_cuuids {
            Some(old_path_cuuids) => {
                let mut new_path_cuuid = Vec::new();
                for cuuid in old_path_cuuids {
                    match map_new_by_old_cuuid.get(&cuuid) {
                        Some(new_cuuid) => new_path_cuuid.push(new_cuuid.clone()),
                        None => break  // We're getting out of the mapped area, break the graph here
                    }
                }

                // Add destination path to remapped directories, this connects the new graph
                new_path_cuuid.extend(destination_path_cuuid.clone());
                new_path_cuuid
            },
            None => destination_path_cuuid.clone()
        };

        // Keep a copy of the path, will be used to map the files. Insert the directory as first entry (to be used with files)
        let mut new_mapped_path = vec![new_tuuid.clone()];
        new_mapped_path.extend(new_path_cuuids.clone());
        map_new_path_by_old_cuuid.insert(old_tuuid.clone(), new_mapped_path);

        // Update the information in the directory entry
        directory.file_rep.tuuid = new_tuuid.to_string();
        directory.file_rep.path_cuuids = Some(new_path_cuuids);

        // Add to transaction entries
        transaction_value.entries.push(directory);
    }

    // Map all files
    let mut filter_files = doc! {
        "$or": [
            { FIELD_TUUID: { "$in": &file_tuuids } },
            { FIELD_PATH_CUUIDS: { "$in": &all_directories } },
        ],
        // FIELD_USER_ID: &user_id_source,
        FIELD_NODE_TYPE: TypeNode::Fichier.to_str(),
    };
    if ! include_deleted {
        filter_files.insert(FIELD_DELETED, false);
    }

    let file_entries = get_complete_copy(mongo, filter_files).await?;
    for mut file in file_entries {
        if transaction_value.entries.len() >= COPY_FILE_BATCH_SIZE {
            let transaction_json = serde_json::to_value(&transaction_value)?;
            debug!("New transaction:\n{}", transaction_json);

            transaction.process_value(
                DOMAIN_NAME,
                TRANSACTION_COPY_V3,
                transaction_json,
                Some(&mut session)
            ).await?;

            batch_counter += 1;
            transaction_value.entries.clear();
            transaction_value.batch_no = Some(batch_counter);
        }

        let cuuid = match file.file_rep.path_cuuids.as_ref() {
            Some(path_cuuids) => match path_cuuids.get(0) {
                Some(cuuid) => cuuid,
                None => return Err(CommonError::Str("File without a path_cuuid"))
            },
            None => return Err(CommonError::Str("File without a path_cuuid"))
        };
        let new_path = match map_new_path_by_old_cuuid.get(cuuid) {
            Some(new_path) => new_path.clone(),
            None => destination_path_cuuid.clone()  // Root of destination
        };

        // Generate a new tuuid for this file
        let new_tuuid = format!("{}{:08x}", tuuid_prefix, tuuid_counter);
        tuuid_counter += 1;

        // Update file
        file.file_rep.tuuid = new_tuuid;
        file.file_rep.path_cuuids = Some(new_path);

        if let Some(media) = file.media.as_mut() {
            media.user_id = user_id.clone();  // Update user_id
        }

        if let Some(comments) = file.comments.as_mut() {
            for comment in comments {
                // Generate new unique identifier for the comment
                let new_comment_id = format!("{}{:08x}", tuuid_prefix, tuuid_counter);
                tuuid_counter += 1;
                comment.comment_id = new_comment_id;
            }
        }

        transaction_value.entries.push(file);
    }

    if ! transaction_value.entries.is_empty() {
        if batch_counter == 1 {
            // No batching used
            transaction_value.batch_no = None;
        }
        let transaction_json = serde_json::to_value(&transaction_value)?;
        debug!("New transaction:\n{}", transaction_json);

        transaction.process_value(
            DOMAIN_NAME,
            TRANSACTION_COPY_V3,
            transaction_json,
            Some(&mut session)
        ).await?;
    }
    drop(transaction_value);

    session.commit_transaction().await?;

    // Emit events on destination directory
    let evenement_contenu = CollectionEventAggregator::new(command.cuuid.as_str())
        .fichiers_ajoutes(command.inclure_tuuids.clone());
    delayed_event_emitter_service.emit(evenement_contenu).await?;

    outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await
}

async fn file_summary<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: TransactionFileSummary = wrapper.message.deserialize()?;

    let tuuid = command.tuuid.as_str();

    // Authorization : Must be in the list of roles
    match wrapper.certificate.verifier_roles_string(vec![ROLE_OLLAMA_RELAI.to_string()])? &&
        wrapper.certificate.verifier_exchanges(vec![Securite::L3Protege])?
    {
        true => (),
        false => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await,
    };

    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    let lease_collection = mongo.get_collection(NOM_COLLECTION_JOBS_LEASES)?;
    let lease_filtre = doc!{ FIELD_TUUID: tuuid, "borrower": CHAMP_FLAG_SUMMARY };
    lease_collection.delete_one(lease_filtre).await?;

    // Emit all tuuids matching this file to ensure listeners get notified
    if let Err(e) = emettre_evenement_maj_fichier(mongo, delayed_event_emitter_service, tuuid).await {
        warn!("command_file_summary Erreur emettre_evenement_maj_fichier {}: {:?}", tuuid, e);
    }

    if let Err(e) = trigger_file_indexing(outbound).await {
        warn!("command_file_summary Error emitting file indexing trigger: {:?}", e);
    }

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

async fn add_web_subtitle<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let command: TransactionAddWebSubtitle = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(e) => return outbound.respond(
            wrapper.delivery_info,
            ErrorMessage::err_code(401, format!("Access denied: {:?}", e).as_str())
        ).await
    };

    // Find matching file
    let collection_reps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre_reps = doc!{ FIELD_FUUIDS_VERSIONS: &command.file_fuuid, CHAMP_USER_ID: &user_id };
    let tuuid = match collection_reps.find_one(filtre_reps).await? {
        Some(row) => row.tuuid,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
    };

    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    if let Err(e) = emettre_evenement_maj_fichier(mongo, delayed_event_emitter_service, tuuid.as_str()).await {
        warn!("command_add_web_subtitle Erreur emettre_evenement_maj_fichier : {:?}", e);
    }

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

async fn remove_web_subtitle<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let command: TransactionRemoveWebSubtitle = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(e) => return outbound.respond(
            wrapper.delivery_info,
            ErrorMessage::err_code(401, format!("Access denied: {:?}", e).as_str())
        ).await
    };

    // Find matching file
    let collection_reps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre_reps = doc!{ FIELD_FUUIDS_VERSIONS: &command.file_fuuid, CHAMP_USER_ID: &user_id };
    let tuuid = match collection_reps.find_one(filtre_reps).await? {
        Some(row) => row.tuuid,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(404, "File not found")).await
    };

    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    if let Err(e) = emettre_evenement_maj_fichier(mongo, delayed_event_emitter_service, tuuid.as_str()).await {
        warn!("command_add_web_subtitle Erreur emettre_evenement_maj_fichier : {:?}", e);
    }

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

#[derive(Deserialize)]
pub struct CommandRecycleItemsV3 {
    tuuids: Vec<String>
}

async fn recycle_items<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: CommandRecycleItemsV3 = wrapper.message.deserialize()?;

    // Authorization, includes checking for shared folder access (impersonating other user_id for request)
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(e) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, format!("Access denied: {:?}", e).as_str())).await
    };

    // Ensure that all tuuids are delete directly (supprime_indirect == false) and that they
    // are part of a hierarchy that is not deleted.
    let collection = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    let mut file_tuuids_by_fuuid: HashMap<String, Vec<String>> = HashMap::new();
    let mut directory_tuuids = Vec::new();

    let filter = doc!{
        FIELD_TUUID: { "$in": &command.tuuids },
        FIELD_USER_ID: &user_id,
        FIELD_DELETED: true,
    };
    let mut cursor = collection.find(filter).await?;
    while let Some(row) = cursor.next().await {
        let item = row?;
        // To recycle a file/directory, it must have been deleted directly (supprime == true, supprime_indirect == false)
        if !(item.supprime == true && item.supprime_indirect == false) {
            let message = format!("Item tuuid:{} not in proper state to recycle", item.tuuid);
            return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, message.as_str())).await
        }

        let type_node = match TypeNode::try_from(item.type_node.as_str()) {
            Ok(inner) => inner,
            Err(_e) => {
                let message = format!("Item tuuid:{} not a supported node type", item.type_node);
                return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, message.as_str())).await
            }
        };

        match type_node {
            TypeNode::Fichier => {
                match item.fuuids_versions.as_ref() {
                    Some(fuuids_versions) => {
                        for fuuid in fuuids_versions.iter() {
                            match file_tuuids_by_fuuid.get_mut(fuuid) {
                                Some(tuuids) => {
                                    tuuids.push(item.tuuid.clone());
                                },
                                None => {
                                    file_tuuids_by_fuuid.insert(fuuid.clone(), vec![item.tuuid.clone()]);
                                }
                            }
                        }
                    },
                    None => {
                        let message = format!("File tuuid:{} has no fuuids", item.tuuid);
                        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, message.as_str())).await
                    }
                }
            }
            _ => {
                directory_tuuids.push(item.tuuid.clone());
            }
        }

        // To recycle a file/directory, its hierarchy must not be deleted.
        if let Some(cuuids) = item.path_cuuids {
            let filtre = doc!{
                FIELD_TUUID: {"$in": cuuids},
                FIELD_USER_ID: &user_id,
            };
            let mut cursor = collection.find(filtre).await?;
            while let Some(row) = cursor.next().await {
                let row = row?;
                if row.supprime {
                    let message = format!("File tuuid:{} is under a deleted path", item.tuuid);
                    return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, message.as_str())).await
                }
            }
        };
    }

    // Ensure all files to restore still exist by claiming them
    let fuuids: Vec<String> = file_tuuids_by_fuuid.keys().into_iter().map(|k| k.to_string()).collect();
    claim_files_by_fuuids(outbound, &fuuids).await?;
    drop(fuuids);
    claim_files_under_deleted_cuuids(mongo, outbound, &user_id, &directory_tuuids).await?;

    let type_node_fichier: &str = TypeNode::Fichier.into();
    let type_node_directory: &str = TypeNode::Repertoire.into();
    let type_node_collection: &str = TypeNode::Collection.into();

    // Make a list of all fuuids indirectly deleted under directories to restore.
    let filtre = doc! {
        CHAMP_PATH_CUUIDS: {"$in": &directory_tuuids},
        CHAMP_USER_ID: &user_id,
        CHAMP_TYPE_NODE: type_node_fichier,
        CHAMP_SUPPRIME: true,
        CHAMP_SUPPRIME_INDIRECT: true,
    };
    let mut cursor = collection.find(filtre).await?;
    while let Some(row) = cursor.next().await {
        let item = row?;
        if let Some(fuuids_versions) = item.fuuids_versions {
            for fuuid in fuuids_versions.iter() {
                match file_tuuids_by_fuuid.get_mut(fuuid) {
                    Some(tuuids) => {
                        tuuids.push(item.tuuid.clone());
                    },
                    None => {
                        file_tuuids_by_fuuid.insert(fuuid.clone(), vec![item.tuuid.clone()]);
                    }
                }
            }
        }
    }

    // List all subdirectories with supprime_indirect == true.
    let filtre = doc!{
        CHAMP_PATH_CUUIDS: {"$in": &directory_tuuids},
        CHAMP_TYPE_NODE: {"$in": vec![type_node_directory, type_node_collection]},
        CHAMP_USER_ID: &user_id,
        CHAMP_SUPPRIME: true,
        CHAMP_SUPPRIME_INDIRECT: true,
    };
    let mut cursor = collection.find(filtre).await?;
    while let Some(row) = cursor.next().await {
        directory_tuuids.push(row?.tuuid);
    }

    // Build the Recycle transaction
    let mut original_command = wrapper.message.clone();
    original_command.certificat = None;
    let transaction_value = TransactionRecycleItemsV4 {
        command: Some(original_command),
        user_id: user_id.clone(),
        file_tuuids_by_fuuid,
        directory_tuuids,
    };

    debug!("command_recycle_items_v3 Transaction\n{}", serde_json::to_string(&transaction_value)?);

    transaction.process_value(
        DOMAIN_NAME,
        TRANSACTION_RECYCLE_ITEMS_V4,
        serde_json::to_value(&transaction_value)?,
        None
    ).await?;

    debug!("command_recycle_items_v3 Post transaction cleanup");
    // Emit recycle events for each file by cuuid.
    {
        let mut files_by_cuuid = HashMap::new();
        let mut tuuids = Vec::new();
        for list_tuuids in transaction_value.file_tuuids_by_fuuid.values() {
            tuuids.extend(list_tuuids);
        }
        let filtre = doc! {
            CHAMP_TUUID: {"$in": &tuuids},
            CHAMP_USER_ID: &user_id,
            CHAMP_SUPPRIME: false,
            CHAMP_TYPE_NODE: type_node_fichier,
        };
        let mut cursor = collection.find(filtre).await?;
        while let Some(row) = cursor.next().await {
            let row = row?;
            let cuuid = match row.path_cuuids {
                Some(path_cuuids) => {
                    match path_cuuids.first() {
                        Some(inner) => inner.to_owned(),
                        None => user_id.clone(),
                    }
                }
                None => user_id.clone()
            };
            let list = match files_by_cuuid.get_mut(&cuuid) {
                Some(list) => list,
                None => {
                    files_by_cuuid.insert(cuuid.clone(), Vec::new());
                    files_by_cuuid.get_mut(&cuuid).unwrap()
                },
            };
            list.push(row.tuuid);
        }

        for (cuuid, files) in files_by_cuuid {
            let event = CollectionEventAggregator::new(cuuid.as_str()).fichiers_ajoutes(files);
            delayed_event_emitter_service.emit(event).await?;
        }
    }

    if transaction_value.directory_tuuids.len() > 0 {
        // List all immediate files/subdirectories
        let filtre = doc! {
            CHAMP_TUUID: {"$in": &transaction_value.directory_tuuids},
            format!("{}.0", CHAMP_PATH_CUUIDS): {"$exists": false},
            CHAMP_USER_ID: &user_id,
            CHAMP_SUPPRIME: false,
        };
        let mut tuuids_collection = Vec::new();
        let mut cursor = collection.find(filtre).await?;
        while let Some(row) = cursor.next().await {
            tuuids_collection.push(row?.tuuid);
        }
        if tuuids_collection.len() > 0 {
            let event = CollectionEventAggregator::new(user_id.as_str()).collections_modifiees(tuuids_collection);
            delayed_event_emitter_service.emit(event).await?;
        }
    }

    // Emit event for each recycled directory
    for cuuid in transaction_value.directory_tuuids.into_iter() {
        let mut fichiers_ajoutes = Vec::new();
        let mut collections_ajoutees = Vec::new();

        // List all immediate files/subdirectories
        let filtre = doc!{
            format!("{}.0", CHAMP_PATH_CUUIDS): &cuuid,
            CHAMP_USER_ID: &user_id,
            CHAMP_SUPPRIME: false,
        };
        let mut cursor = collection.find(filtre).await?;
        while let Some(row) = cursor.next().await {
            let row = row?;
            let type_node = TypeNode::try_from(row.type_node.as_str())?;
            match type_node {
                TypeNode::Fichier => fichiers_ajoutes.push(row.tuuid),
                _ => collections_ajoutees.push(row.tuuid),
            }
        }

        // Update the directory itself (deleted flags)
        emit_rep_update_event(mongo, outbound, delayed_event_emitter_service, cuuid.as_str(), user_id.as_str()).await?;

        // Update content (files and sub-directories)
        let event = CollectionEventAggregator::new(cuuid.as_str())
            .fichiers_ajoutes(fichiers_ajoutes)
            .collections_ajoutees(collections_ajoutees);
        delayed_event_emitter_service.emit(event).await?;
    }

    outbound.respond(wrapper.delivery_info, ErrorMessage::ok()).await
}


async fn delete_image_job<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let command: TransactionSupprimerJobImageV2 = wrapper.message.deserialize()?;

    if ! wrapper.certificate.verifier_roles(vec![RolesCertificats::Media])?
        && wrapper.certificate.verifier_exchanges(vec![Securite::L2Prive])?
    {
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Access denied")).await
    }

    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    // Remove lease
    let lease_collection = mongo.get_collection(NOM_COLLECTION_JOBS_VERSIONS_LEASES)?;
    let lease_filtre = doc!{ FIELD_FUUID: &command.fuuid, "borrower": CHAMP_FLAG_MEDIA_TRAITE };
    lease_collection.delete_one(lease_filtre).await?;

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

#[derive(Clone, Debug, Deserialize)]
struct CommandeAjouterContactLocal {
    nom_usager: String,
}

#[derive(Serialize)]
struct CommandeAjouterContactLocalReponse {
    ok: bool,
    nom_usager: String,
    user_id: String,
    contact_id: String
}

#[derive(Serialize)]
struct RequestGetUserInformation {
    noms_usagers: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ReponseChargerUserIdParNomUsager {
    usagers: Option<HashMap<String, Option<String>>>
}

async fn share_add_contact<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: CommandeAjouterContactLocal = wrapper.message.deserialize()?;
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(_) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Acces denied")).await
    };

    // Identifier le user_id de l'usager a ajouter
    let user_contact_id = {
        let routage = RoutageMessageAction::builder(
            DOMAINE_NOM_MAITREDESCOMPTES,
            "getUserIdParNomUsager",
            vec![Securite::L3Protege]
        )
            .timeout_blocking(4_000)
            .build();

        let request = RequestGetUserInformation { noms_usagers: vec![command.nom_usager.clone()] };
        let response: ReponseChargerUserIdParNomUsager = match outbound.send_request(routage, request).await {
            Ok(response) => {
                match response.is_err()? {
                    (true, err) => {
                        error!("Error requesting user information: {:?}", err);
                        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Error requesting user information")).await
                    },
                    _ => response.message.deserialize()?
                }
            },
            Err(e) => {
                error!("Error requesting user informatino: {:?}", e);
                return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Timeout requesting user information")).await
            }
        };

        match response.usagers {
            Some(mut inner) => {
                match inner.remove(command.nom_usager.as_str()) {
                    Some(inner) => match inner {
                        Some(inner) => inner,
                        None => {
                            debug!("commande_ajouter_contact_local Erreur chargement user_id pour contact (usager inconnu - 1), SKIP");
                            // return Ok(Some(middleware.formatter_reponse(&json!({"ok": false, "err": "Erreur chargement user_id pour contact local"}), None)?))
                            return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Error processing user information")).await
                        }
                    },
                    None => {
                        debug!("commande_ajouter_contact_local Erreur chargement user_id pour contact (usager inconnu - 2), SKIP");
                        // return Ok(Some(middleware.formatter_reponse(&json!({"ok": false, "err": "Erreur chargement user_id pour contact local"}), None)?))
                        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Error processing user information")).await
                    }
                }
            },
            None => {
                debug!("commande_ajouter_contact_local Erreur chargement user_id pour contact (reponse sans liste usagers), SKIP");
                // return Ok(Some(middleware.formatter_reponse(&json!({"ok": false, "err": "Erreur chargement user_id pour contact local"}), None)?))
                return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(500, "Error processing user information")).await
            }
        }
    };

    if user_contact_id == user_id {
        debug!("commande_ajouter_contact_local Usager (courant) tente de s'ajouter a ses propres contacts, SKIP");
        // return Ok(Some(middleware.formatter_reponse(&json!({"ok": false, "err": "Usager courant ne peut etre ajoute au contacts"}), None)?))
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(400, "Not allowed - trying to add same user to contacts")).await
    }

    // Check if contact already exists
    let collection = mongo.get_collection(NOM_COLLECTION_PARTAGE_CONTACT)?;
    let filtre_current = doc!{ FIELD_USER_ID: &user_id, "contact_user_id": &user_contact_id};
    if collection.find_one(filtre_current).await?.is_some() {
        debug!("Share already exists for user_id: {}, contact user id: {}", user_id, user_contact_id);
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(409, "Share already exists")).await
    }

    // Convertir en transaction
    let transaction_value = TransactionAjouterContactLocal { user_id, contact_user_id: user_contact_id };
    transaction.process_value(DOMAIN_NAME, TRANSACTION_AJOUTER_CONTACT_LOCAL, serde_json::to_value(&transaction_value)?, None).await?;

    // Respond with transaction information
    let response = CommandeAjouterContactLocalReponse {
        ok: true,
        nom_usager: command.nom_usager,
        user_id: transaction_value.user_id,
        contact_id: transaction_value.contact_user_id
    };

    outbound.respond(wrapper.delivery_info, response).await
}

async fn share_remove_contact<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: TransactionSupprimerContacts = wrapper.message.deserialize()?;
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(_) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Acces denied")).await
    };

    let filter = doc! {
        CHAMP_USER_ID: &user_id,
        CHAMP_CONTACT_ID: {"$in": &command.contact_ids},
    };
    let collection_contacts = mongo.get_collection(NOM_COLLECTION_PARTAGE_CONTACT)?;
    let count = collection_contacts.count_documents(filter).await? as usize;
    if count != command.contact_ids.len() {
        debug!("User {} requesting removal of contact ids {:?}, mismatch with owned (count: {})", user_id, command.contact_ids, count);
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Acces denied")).await
    }

    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

async fn share_add_collection<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: TransactionPartagerCollections = wrapper.message.deserialize()?;
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(_) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Acces denied")).await
    };

    let filter = doc!{ FIELD_TUUID: { "$in": &command.cuuids }, FIELD_USER_ID: &user_id };
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let count = collection.count_documents(filter).await? as usize;
    if command.cuuids.len() != count {
        debug!("User {} requesting shares on {:?}, mismatch with owned (count: {})", user_id, command.cuuids, count);
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Acces denied")).await
    }

    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

async fn share_remove_collection<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    transaction: &FileManagerTransactionService,
    wrapper: MessageValidated
) -> Result<(), CommonError> where M: MongoDaoTyped
{
    let command: TransactionSupprimerPartageUsager = wrapper.message.deserialize()?;
    let user_id = match verify_user_certificate(wrapper.certificate.as_ref()) {
        Ok(user_id) => user_id,
        Err(_) => return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Acces denied")).await
    };

    let filter = doc!{ FIELD_TUUID: { "$in": &command.tuuid }, FIELD_USER_ID: &user_id };
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let count = collection.count_documents(filter).await? as usize;
    if count != 1 {
        debug!("User {} requesting removal of share on {:?} but not owner", user_id, command.tuuid);
        return outbound.respond(wrapper.delivery_info, ErrorMessage::err_code(401, "Acces denied")).await
    }

    let delivery_info = wrapper.delivery_info.clone();
    transaction.process_transaction(wrapper.into(), None).await?;

    outbound.respond(delivery_info, ErrorMessage::ok()).await
}

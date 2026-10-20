use crate::constants::*;
use crate::external::mongo::*;
use crate::external::mq::*;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::{VerificateurPermissions, calculer_fingerprint_pk, charger_csr, csr_calculer_fingerprintpk, get_csr_subject};
use millegrilles_common_rust::chrono::Utc;
use millegrilles_common_rust::constantes::{DELEGATION_GLOBALE_PROPRIETAIRE, Securite};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::{MessageKind, MessageValidable};
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::serde_json::json;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::v3::{ConfigService, DocumentEncryptionService, PkiService};
use millegrilles_common_rust::{bson, chrono, hex, serde_json};
use serde::{Deserialize, Serialize};
use crate::flow::transactions::FileManagerTransactionService;

pub async fn process_command<M,E>(
    config: &dyn ConfigService,
    pki: &dyn PkiService,
    encryption: &E,
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated,
) -> Result<(), CommonError> where M: MongoDaoTyped, E: DocumentEncryptionService {
    let action = match wrapper.get_routing_action() {
        Some(action) => action,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err("No action provided in command")).await
    };

    match wrapper.message.kind {
        MessageKind::Commande => {
            match action {
                // COMMANDE_SIGNER_COMPTEUSAGER => sign_user_account(config, pki, mongo, outbound, wrapper).await,
                _ => {
                    info!("Unknown action {} for process_command, skipping", action);
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

/// Process the command part of the transaction (checks, validations, volatile updates),
/// calls transaction processor and then handles responses and emits events.
pub async fn process_transaction<M,E>(
    config: &dyn ConfigService,
    mongo: &M,
    outbound: &MessageOutboundFacade,
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
                // TRANSACTION_INSCRIRE_USAGER => register_user(mongo, outbound, transaction, wrapper).await,
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

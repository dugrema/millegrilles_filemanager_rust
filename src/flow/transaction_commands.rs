use crate::flow::transactions::FileManagerTransactionService;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::tracing::info;
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::v3::{ConfigService, DocumentEncryptionService};

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
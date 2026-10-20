use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageKind;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::tracing::info;
use millegrilles_common_rust::v3::ConfigService;
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::models::ErrorMessage;

pub async fn process_request<M>(
    config: &dyn ConfigService,
    mongo: &M,
    outbound: &MessageOutboundFacade,
    wrapper: MessageValidated,
) -> Result<(), CommonError> where
    M: MongoDaoTyped,
{
    let action = match wrapper.get_routing_action() {
        Some(action) => action,
        None => return outbound.respond(wrapper.delivery_info, ErrorMessage::err("No action provided in request")).await
    };

    match wrapper.message.kind {
        MessageKind::Requete => {
            match action {
                // REQUETE_CHARGER_USAGER => request_load_user(config, mongo, outbound, wrapper).await,

                _ => {
                    info!("Unknown action {} for process_request, skipping", action);
                    Ok(())
                }
            }
        },
        _ => {
            info!("Unhandled message type with action {} for process_request, skipping", action);
            Ok(())
        }
    }
}

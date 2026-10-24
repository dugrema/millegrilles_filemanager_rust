use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::constantes::Securite;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::tracing::{error, info, warn};
use millegrilles_common_rust::v3::ChiffrageService;
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use serde::Deserialize;
use crate::constants::DOMAINE_FILECONTROLER_NOM;
use crate::external::mq::*;
use crate::flow::file_events::{declencher_traitement_nouveau_fuuid, marquer_visites_fuuids, DelayedEventEmitterService};

pub async fn process_events<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    wrapper: MessageValidated,
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let action = match wrapper.get_routing_action() {
        Some(action) => action,
        None => {
            warn!("No action provided in event");
            return Ok(());
        }
    };
    match action {
        EVENEMENT_FILEHOST_NEWFUUID => filehost_new_fuuid(mongo, outbound, delayed_event_emitter_service, wrapper).await,
        _ => {
            warn!("Unknown action {} for process_events, skipping", action);
            Ok(())
        }
    }
}

#[derive(Deserialize)]
struct FilecontrolerNewFuuidEvent {
    filehost_id: String,
    fuuid: String,
}

async fn filehost_new_fuuid<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    wrapper: MessageValidated,
) -> Result<(), CommonError> where M: MongoDaoTyped {

    if !wrapper.certificate.verifier_exchanges(vec![Securite::L1Public])? {
        error!("evenement_filehost_newfuuid Acces refuse, certificat n'est pas d'un exchange L2");
        return Ok(())
    }
    if !wrapper.certificate.verifier_roles_string(vec![DOMAINE_FILECONTROLER_NOM.into()])? {
        error!("evenement_filehost_newfuuid Acces refuse, certificat n'est pas de role filecontroler");
        return Ok(())
    }

    let event: FilecontrolerNewFuuidEvent = wrapper.message.deserialize()?;

    // Expose fields
    let fuuid = event.fuuid.as_str();
    let filehost_id = event.filehost_id.as_str();
    let visit_date = &wrapper.message.estampille;

    // Marquer la visite courante
    marquer_visites_fuuids(mongo, &vec![fuuid], visit_date, filehost_id).await?;

    declencher_traitement_nouveau_fuuid(mongo, outbound, delayed_event_emitter_service, fuuid, &vec![filehost_id]).await?;

    Ok(())
}

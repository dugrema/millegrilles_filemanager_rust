use millegrilles_common_rust::bson;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::constantes::{Securite, TOPOLOGIE_NOM_DOMAINE};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::v3::ChiffrageService;
use millegrilles_common_rust::v3::facades::message_inbound::MessageValidated;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use serde::Deserialize;
use crate::constants::DOMAINE_FILECONTROLER_NOM;
use crate::external::mongo::*;
use crate::external::mq::*;
use crate::flow::file_events::{declencher_traitement_nouveau_fuuid, marquer_visites_fuuids, DelayedEventEmitterService};

pub async fn process_events<M>(
    chiffrage: &dyn ChiffrageService,
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
        EVENT_KEYMASTER_CERTIFICATE => save_keymaster_certificate(chiffrage, wrapper).await,
        EVENEMENT_FILEHOST_NEWFUUID => filehost_new_fuuid(mongo, outbound, delayed_event_emitter_service, wrapper).await,
        EVENEMENT_RESET_VISITS_CLAIMS => reset_visits_claims(mongo, wrapper).await,
        _ => {
            warn!("Unknown action {} for process_events, skipping", action);
            Ok(())
        }
    }
}

async fn save_keymaster_certificate(
    chiffrage: &dyn ChiffrageService,
    wrapper: MessageValidated,
) -> Result<(), CommonError> {
    debug!("Saving keymaster certificate for encryption/fiche");
    if let Err(e) = chiffrage.add_encryption_publickey(wrapper.certificate) {
        warn!("Error saving keymaster certificate: {:?}", e);
    }
    Ok(())
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
    debug!("filehost_new_fuuid {} on {}", event.fuuid, event.filehost_id);

    // Expose fields
    let fuuid = event.fuuid.as_str();
    let filehost_id = event.filehost_id.as_str();
    let visit_date = &wrapper.message.estampille;

    // Marquer la visite courante
    marquer_visites_fuuids(mongo, &vec![fuuid], visit_date, filehost_id).await?;

    declencher_traitement_nouveau_fuuid(mongo, outbound, delayed_event_emitter_service, fuuid, &vec![filehost_id]).await?;

    Ok(())
}

async fn reset_visits_claims<M>(
    mongo: &M,
    wrapper: MessageValidated,
) -> Result<(), CommonError> where M: MongoDaoTyped {

    if !wrapper.certificate.verifier_exchanges(vec![Securite::L3Protege])? {
        error!("evenement_filehost_newfuuid Acces refuse, certificat n'est pas d'un exchange L3");
        return Ok(())
    }
    if !wrapper.certificate.verifier_domaines(vec![TOPOLOGIE_NOM_DOMAINE.into()])? {
        error!("evenement_filehost_newfuuid Acces refuse, certificat n'est pas de role filecontroler");
        return Ok(())
    }

    let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let date_initial_verification = bson::datetime::DateTime::from_millis(1_704_085_200_000);
    let ops = doc! {
        "$set": { CONST_FIELD_LAST_VISIT_VERIFICATION: date_initial_verification },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    collection_versions.update_many(doc!{}, ops).await?;

    info!("Visits/claims reset done");

    Ok(())
}

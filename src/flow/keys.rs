use millegrilles_common_rust::common_messages::{RequeteDechiffrage, ResponseRequestDechiffrageV2Cle};
use millegrilles_common_rust::constantes::{Securite, COMMANDE_AJOUTER_CLE_DOMAINES, DOMAINE_NOM_MAITREDESCLES, MAITREDESCLES_REQUETE_DECHIFFRAGE_V2};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::{MessageKind, MessageMilleGrillesOwned};
use millegrilles_common_rust::v3::{FormatService, MessagingService};
use millegrilles_common_rust::v3::models::{DecryptedKey, ErrorMessage};
use std::sync::Arc;
use millegrilles_common_rust::millegrilles_cryptographie::x509::EnveloppeCertificat;
use millegrilles_common_rust::serde_json;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::impls::rabbitmq_consumer::DeliveryInfo;
use crate::constants::DOMAIN_NAME;

/// Convert to older key format
pub fn convert_decrypted_key_into_v2(key: DecryptedKey) -> Result<ResponseRequestDechiffrageV2Cle, CommonError> {
    Ok(ResponseRequestDechiffrageV2Cle {
        cle_secrete_base64: key.key.cle_secrete_base64.to_string(),
        cle_id: match key.key.cle_id.as_ref() { Some(value) => Some(value.to_string()), None => None },
        format: key.key.format.clone(),
        nonce: match key.key.nonce.as_ref() { Some(value) => Some(value.to_string()), None => None },
        verification: match key.key.verification.as_ref() { Some(value) => Some(value.to_string()), None => None },
        signature: key.signature,
    })
}

pub fn convert_decrypted_key_vec_into_v2(keys: Vec<DecryptedKey>) -> Result<Vec<ResponseRequestDechiffrageV2Cle>, CommonError> {
    let mut converted_keys = Vec::with_capacity(keys.len());
    for key in keys {
        converted_keys.push(convert_decrypted_key_into_v2(key)?);
    }
    Ok(converted_keys)
}

/// Relays the key message to the keymaster. Does not change/sign/verify the message.
/// Raise error if keymaster reports failure.
pub async fn relay_attached_key(
    messaging: &dyn MessagingService,
    key_message: MessageMilleGrillesOwned
) -> Result<(), CommonError> {
    let routing = RoutageMessageAction::builder(
        DOMAINE_NOM_MAITREDESCLES, COMMANDE_AJOUTER_CLE_DOMAINES, vec![Securite::L1Public])
        .correlation_id(&key_message.id)
        .timeout_blocking(5_000)
        .build();
    let result = messaging.send(key_message.try_into()?, routing).await?;
    match result.is_err()? {
        (true, e) => Err(CommonError::String(format!("Error saving key: {:?}", e))),
        (false, _) => Ok(())
    }
}

pub async fn allow_key_decryption(
    messaging: &dyn MessagingService,
    format: &dyn FormatService,
    key_ids: Vec<String>,
    delivery_info: DeliveryInfo,
    certificate: &EnveloppeCertificat
) -> Result<(), CommonError> {

    // let cle_id = inner.cle_id;
    let request = RequeteDechiffrage {
        domaine: DOMAIN_NAME.to_string(),
        liste_hachage_bytes: None,
        cle_ids: Some(key_ids),
        certificat_rechiffrage: Some(certificate.chaine_pem()?),
        inclure_signature: None,
    };

    // Extract reply information, keymaster will reply directly
    let reply_properties = &delivery_info.properties;
    let (reply_to, correlation_id) = match (reply_properties.reply_to(), reply_properties.correlation_id()) {
        (Some(reply_to), Some(correlation_id)) => (reply_to.to_string(), correlation_id.to_string()),
        _ => return Err(CommonError::Str("Missing reply information"))
    };

    let routing = RoutageMessageAction::builder(
        DOMAINE_NOM_MAITREDESCLES,
        MAITREDESCLES_REQUETE_DECHIFFRAGE_V2,
        vec![Securite::L3Protege]
    )
        .reply_to(reply_to)
        .correlation_id(correlation_id)
        .blocking(false)
        .build();

    // The response is handled by KeyMaster. Emit a request (do not wait for response)
    let value = serde_json::to_value(request)?;
    let (response, _id) = format.build_action_message(
        MessageKind::Requete, &routing, value)?;

    messaging.emit(response, Some(routing)).await
}

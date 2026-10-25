use millegrilles_common_rust::common_messages::ResponseRequestDechiffrageV2Cle;
use millegrilles_common_rust::constantes::{COMMANDE_AJOUTER_CLE_DOMAINES, DOMAINE_NOM_MAITREDESCLES, Securite};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::MessageMilleGrillesOwned;
use millegrilles_common_rust::v3::MessagingService;
use millegrilles_common_rust::v3::models::DecryptedKey;
use std::sync::Arc;

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

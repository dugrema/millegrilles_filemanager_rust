use millegrilles_common_rust::common_messages::ResponseRequestDechiffrageV2Cle;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::v3::models::DecryptedKey;

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

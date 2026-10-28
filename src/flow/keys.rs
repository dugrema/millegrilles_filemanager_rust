use std::collections::{HashMap, HashSet};
use millegrilles_common_rust::common_messages::{RequeteDechiffrage, ResponseRequestDechiffrageV2Cle};
use millegrilles_common_rust::constantes::{Securite, COMMANDE_AJOUTER_CLE_DOMAINES, DOMAINE_NOM_MAITREDESCLES, MAITREDESCLES_REQUETE_DECHIFFRAGE_V2};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::millegrilles_cryptographie::messages_structs::{MessageKind, MessageMilleGrillesOwned};
use millegrilles_common_rust::v3::{FormatService, MessagingService};
use millegrilles_common_rust::v3::models::{DecryptedKey, ErrorMessage};
use std::sync::Arc;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage::FormatChiffrage;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage_cles::CleSecreteSerialisee;
use millegrilles_common_rust::millegrilles_cryptographie::x509::EnveloppeCertificat;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::serde_json;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::impls::rabbitmq_consumer::DeliveryInfo;
use serde::Serialize;
use crate::constants::DOMAIN_NAME;
use crate::external::mongo::*;
use crate::external::mongo_struct::{MediaOwnedRow, NodeFichierVersionOwned};

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

#[derive(Serialize)]
pub struct FileDecryptionInformation {
    fuuid: String,
    decryption: CleSecreteSerialisee,
    filesize: u64,
    mimetype: String,
}

pub async fn fetch_decryption_information<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    fuuids: &Vec<String>,
) -> Result<HashMap<String, FileDecryptionInformation>, CommonError> where M: MongoDaoTyped {
    // Start by fetching all decryption info for the matching fuuids. Some may be in the media table.

    let mut response_map = HashMap::with_capacity(fuuids.len());
    let mut media_fuuids = Vec::new();  // List of video fuuids, need to look-up in media collection

    // Build map of fuuids with their key, inject decryption information in keys (nonce, format).
    let collection_versions = mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;
    let filter = doc! { CHAMP_FUUIDS_RECLAMES: { "$in": fuuids } };
    let mut cursor = collection_versions.find(filter).await?;
    while let Some(row) = cursor.next().await {
        let row = row?;

        if fuuids.contains(&row.fuuid) {
            // Information is being requested for the original, create an entry
            response_map.insert(
                row.fuuid.clone(),
                FileDecryptionInformation {
                    fuuid: row.fuuid.clone(),
                    decryption: CleSecreteSerialisee {
                        cle_secrete_base64: Default::default(), // Placeholder
                        cle_id: match row.cle_id {
                            Some(inner) => Some(inner.as_str().try_into().map_err(|()| CommonError::Str("Error mapping key_id to 128 chars"))?),
                            None => return Err(CommonError::Str("Missing key_id decryption information"))
                        },
                        format: row.format,
                        nonce: match row.nonce { Some(inner) => Some(inner.as_str().try_into().map_err(|()| CommonError::Str("Error mapping nonce to 64 chars"))?), None => None },
                        verification: match row.verification { Some(inner) => Some(inner.as_str().try_into().map_err(|()| CommonError::Str("Error mapping nonce to 128 chars"))?), None => None },
                    },
                    filesize: row.taille,
                    mimetype: row.mimetype,
                }
            );
        }

        if row.flag_media == Some(VALUE_MEDIA_VIDEO.to_string()) {
            // Also check the media table for transcoded versions of this video
            media_fuuids.push(row.fuuid.clone());
        }
    }

    // Only look-up the media table when necessary (e.g. keys are missing and there are media fuuids)
    if response_map.len() < fuuids.len() && ! media_fuuids.is_empty() {
        let filter = doc!{ FIELD_FUUID: { "$in": &media_fuuids } };
        let collection_media = mongo.get_collection_typed::<MediaOwnedRow>(NOM_COLLECTION_MEDIA)?;
        let mut cursor = collection_media.find(filter).await?;
        while let Some(row) = cursor.next().await {
            let row = row?;

            // Only check videos, the other types of media (images, audio) are not supported.
            if let Some(video) = row.video {
                for fuuid in fuuids {
                    if let Some(value) = video.get(fuuid) {
                        let format: Option<FormatChiffrage> = match value.format.as_ref() {
                            Some(inner) => Some(inner.as_str().try_into().map_err(|_| CommonError::Str("Error mapping encryption format"))?),
                            None => None
                        };

                        response_map.insert(
                            value.fuuid_video.clone(),
                            FileDecryptionInformation {
                                fuuid: value.fuuid_video.clone(),
                                decryption: CleSecreteSerialisee {
                                    cle_secrete_base64: Default::default(), // Placeholder
                                    cle_id: match value.cle_id.as_ref() {
                                        Some(inner) => Some(inner.as_str().try_into().map_err(|()| CommonError::Str("Error mapping key_id to 128 chars"))?),
                                        None => return Err(CommonError::Str("Missing key_id decryption information"))
                                    },
                                    format,
                                    nonce: match value.nonce.as_ref() {
                                        Some(inner) => Some(inner.as_str().try_into().map_err(|()| CommonError::Str("Error mapping nonce to 64 chars"))?),
                                        None => None
                                    },
                                    verification: None,
                                },
                                filesize: value.taille_fichier,
                                mimetype: value.mimetype.clone(),
                            }
                        );
                    }
                }
            }
        }
    }

    if response_map.len() < fuuids.len() {
        return Err(CommonError::Str("Unable to find all requested files"))
    }

    // Make a deduped list of key_ids
    let mut key_ids = HashSet::with_capacity(fuuids.len());
    for value in response_map.values() {
        if let Some(key_id) = value.decryption.cle_id.as_ref() {
            key_ids.insert(key_id.to_string());
        }
    }

    // Retrieve decryption keys
    let keys = outbound.get_keys(DOMAIN_NAME, key_ids.into_iter().collect(), None).await?;

    // Create map of response keys, by cle_id.
    let mut decrypted_key_map = HashMap::with_capacity(keys.len());
    for key in keys {
        if let Some(cle_id) = key.key.cle_id.as_ref() {
            decrypted_key_map.insert(cle_id.clone(), key);
        }
    }

    // Inject decrypted keys to matching key_ids in the map
    for value in response_map.values_mut() {
        if let Some(cle_id) = value.decryption.cle_id.as_ref() {
            match decrypted_key_map.get(cle_id) {
                Some(key) => {
                    value.decryption.cle_secrete_base64 = key.key.cle_secrete_base64.clone();
                },
                None => return Err(CommonError::Str("Missing decryption information from keymaster response"))
            }
        }
    }

    Ok(response_map)
}

use crate::constants::DOMAIN_NAME;
use crate::external::mongo::{NOM_COLLECTION_FICHIERS_REP, NOM_COLLECTION_MEDIA, NOM_COLLECTION_VERSIONS};
use crate::external::mongo_struct::{MediaOwnedRow, NodeFichierVersionOwned, VideoDetail};
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::common_messages::InformationDechiffrageV2;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::millegrilles_cryptographie::chiffrage::FormatChiffrage;
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::v3::ConfigService;
use serde::{Deserialize, Serialize};

pub struct InformationFichierStream {
    pub mimetype: String,
    pub dechiffrage: InformationDechiffrageV2,
}

pub async fn get_information_fichier_stream<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    user_id: &str,
    fuuid: &str,
    fuuid_ref: Option<&str>,
) -> Result<InformationFichierStream, CommonError> where
    M: MongoDaoTyped,
{
    let (fuuid_original, fuuid_video) = match fuuid_ref.as_ref() {
        Some(inner) => (*inner, Some(fuuid)),
        None => (fuuid.as_ref(), None)
    };

    let resultat = match fuuid_video {
        Some(fuuid_video) => {
            // On a un fuuid de reference (fuuid == video transcode).
            // Fuuid_ref est l'original. Charger en confirmant le user_id de la collection media.
            let filtre = doc! { "fuuid": fuuid_original, "user_id": user_id };
            let collection = mongo.get_collection_typed::<MediaOwnedRow>(NOM_COLLECTION_MEDIA)?;
            let fichier = match collection.find_one(filtre).await? {
                Some(inner) => inner,
                None => Err(format!("requetes.get_information_fichier_stream Fuuid media inconnu {} pour user_id {}", fuuid_original, user_id))?
            };

            // Trouver video correspondant au fuuid.
            let video = match fichier.video {
                Some(inner) => {
                    let mut video_trouve: Vec<VideoDetail> = inner.into_iter()
                        .filter(|f| f.1.fuuid_video.as_str() == fuuid_video)
                        .map(|f| f.1)
                        .collect();
                    match video_trouve.pop() {
                        Some(inner) => inner,
                        None => Err(format!("requetes.get_information_fichier_stream Aucun video avec fuuid_video {} pour fuuid_original {}",
                                            fuuid_video, fuuid_original))?
                    }
                },
                None => Err(format!("requetes.get_information_fichier_stream Aucuns videos pour fuuid_original {}", fuuid_original))?
            };

            let format = match video.format.as_ref() {
                Some(inner) => FormatChiffrage::try_from(inner.as_str())?,
                None => Err(format!("requetes.get_information_fichier_stream Format chiffrage video manquant pour fuuid_video {}", fuuid_video))?
            };

            let cle_id = match video.cle_id {
                Some(inner) => inner,
                None => fuuid_original.to_owned()
            };

            let nonce = match video.nonce {
                Some(inner) => Some(inner),
                None => match video.header {
                    Some(inner) => Some(inner[1..].to_string()),
                    None => None
                }
            };

            let info_dechiffrage = InformationDechiffrageV2 {
                cle_id,
                format,
                nonce,
                verification: None,
                fuuid: Some(fuuid_original.to_owned()),  // Reference au fichier video, requis pour JWT
            };

            InformationFichierStream {
                mimetype: video.mimetype,
                dechiffrage: info_dechiffrage,
            }
        },
        None => {
            // On n'a pas de fuuid de reference
            // Load decryption information
            let filtre = doc! { "fuuids_reclames": fuuid_original };
            let collection = mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;
            let fichier_version = match collection.find_one(filtre).await? {
                Some(inner) => inner,
                None => Err(format!("requetes.get_information_fichier_stream Version fuuid inconnue {}", fuuid_original))?
            };

            // let fuuid_original = fichier_version.fuuid.as_str();

            // Security verification
            let filtre = doc! { "fuuids_versions": fuuid_original, "user_id": user_id };
            let collection = mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_FICHIERS_REP)?;
            let count = collection.count_documents(filtre).await?;
            if count == 0 {
                Err(format!("requetes.get_information_fichier_stream Access denied for user_id {} to fuuid {}", user_id, fuuid_original))?
            }

            // Verifier si on a l'ancien ou le nouveau format de chiffrage symmetrique (V2)
            match fichier_version.cle_id {
                Some(cle_id) => {
                    // Nouveau format, toute l'information est deja disponible
                    let format = match fichier_version.format {
                        Some(inner) => inner,
                        None => Err(CommonError::Str("requetes.get_information_fichier_stream Format de chiffrage manquant"))?
                    };
                    let info_dechiffrage = InformationDechiffrageV2 {
                        cle_id,
                        format,
                        nonce: fichier_version.nonce,
                        verification: fichier_version.verification,
                        fuuid: None,
                    };
                    InformationFichierStream {
                        mimetype: fichier_version.mimetype,
                        dechiffrage: info_dechiffrage,
                    }
                },
                None => {
                    let keys = outbound.get_keys(DOMAIN_NAME, vec![fuuid_original.to_string()], None).await?;
                    let dechiffrage = match keys.get(0) {
                        Some(key) => {
                            let serialized_key = &key.key;
                            InformationDechiffrageV2 {
                                format: match serialized_key.format.as_ref() { Some(inner) => inner.clone(), None => FormatChiffrage::MGS4 },
                                cle_id: match serialized_key.cle_id.as_ref() { Some(inner) => inner.to_string(), None => fuuid_original.to_string() },
                                nonce: match serialized_key.nonce.as_ref() { Some(inner) => Some(inner.to_string()), None => None },
                                verification: match serialized_key.verification.as_ref() { Some(inner) => Some(inner.to_string()), None => None },
                                fuuid: None,
                            }
                        },
                        None => return Err(CommonError::Str("Unknown key for requested file"))
                    };
                    InformationFichierStream {
                        mimetype: fichier_version.mimetype,
                        dechiffrage,
                    }
                }
            }

        }
    };

    Ok(resultat)
}

#[derive(Debug, Serialize, Deserialize)]
struct ClaimsTokenFichier {
    #[serde(rename="userId", skip_serializing_if="Option::is_none")]
    user_id: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    mimetype: Option<String>,

    // Dechiffrage
    #[serde(rename="ref", skip_serializing_if="Option::is_none")]
    ref_: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    nonce: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    verification: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    format: Option<String>
}

pub fn generer_jwt(
    config: &dyn ConfigService,
    user_id: &str,
    fuuid: &str,
    mimetype: &str,
    dechiffrage: InformationDechiffrageV2
) -> Result<String, CommonError> where {
    todo!()
    // let user_id = user_id.to_string();
    // let fuuid = fuuid.to_string();
    // let mimetype = mimetype.to_string();
    //
    // let ref_fuuid = match dechiffrage.fuuid {
    //     Some(inner) => Some(inner),
    //     None => None
    // };
    //
    // let format_chiffrage_string: &str = dechiffrage.format.into();
    // let info_fichier = ClaimsTokenFichier {
    //     user_id: Some(user_id),
    //     mimetype: Some(mimetype),
    //     ref_: ref_fuuid,
    //     nonce: dechiffrage.nonce,
    //     format: Some(format_chiffrage_string.to_string()),
    //     verification: dechiffrage.verification,
    // };
    //
    // let mut claims = Claims::with_custom_claims(
    //     info_fichier, coarsetime::Duration::from_secs(CONST_DUREE_TOKEN_VALIDE));
    // claims.subject = Some(fuuid);
    //
    // // Recuperer cle pour signer le token
    // let enveloppe = config.get_configuration_pki().get_enveloppe_privee();
    // claims.issuer = Some(DOMAINE_NOM_GROSFICHIERS.into());
    // let cle_privee = enveloppe.cle_privee.private_key_to_der()?;
    // let cle_der = match Ed25519KeyPair::from_der(cle_privee.as_slice()) {
    //     Ok(inner) => inner,
    //     Err(e) => Err(format!("Ed25519KeyPair::from_der : {:?}",e ))?
    // };
    // let cle_signature = cle_der.with_key_id(enveloppe.fingerprint()?.as_str());
    //
    // // Signer et retourner le nouveau token
    // let jwt_token = match cle_signature.sign(claims) {
    //     Ok(inner) => inner,
    //     Err(e) => Err(format!("Erreur cle_signature.sign(claims) : {:?}",e ))?
    // };
    // Ok(jwt_token)
}

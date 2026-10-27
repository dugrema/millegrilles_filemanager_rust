use std::collections::HashSet;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::certificats::VerificateurPermissions;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::millegrilles_cryptographie::x509::EnveloppeCertificat;
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::debug;
use millegrilles_common_rust::v3::models::ErrorMessage;
use crate::constants::ROLE_USAGER;
use crate::external::mongo::*;
use crate::external::mongo_struct::{MediaOwnedRow, NodeFichierRepOwned, NodeFichierVersionOwned};

pub fn verify_user_certificate(certificate: &EnveloppeCertificat) -> Result<String, CommonError> {
    let user_id = match certificate.get_user_id()? {
        Some(user_id) => user_id,
        None => return Err(CommonError::Str("No user id"))
    };
    if !certificate.verifier_roles_string(vec![ROLE_USAGER.to_string()])? {
        return Err(CommonError::Str("Not a user certificate"))
    };
    Ok(user_id)
}

pub async fn verify_user_access_tuuids(mongo: &dyn MongoDao, user_id: &str, tuuids: &Vec<String>) -> Result<bool, CommonError> {
    let filter = doc!{ FIELD_TUUID: { "$in": tuuids }, FIELD_USER_ID: user_id };
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let count = collection.count_documents(filter).await? as usize;
    Ok(tuuids.len() == count)
}

pub async fn verifier_acces_usager_media<M>(
    mongo: &M,
    user_id: &str,
    fuuids: &Vec<&str>
) -> Result<Vec<String>, CommonError> where M: MongoDaoTyped {
    debug!("verifier_acces_usager_media Requested fuuids : {:?}", fuuids);

    // Build list of all original fuuids for this user on requested fuuids (may be images or videos).
    let verified_fuuids = {
        let mut original_fuuids = Vec::new();
        let filtre = doc! { "fuuids_reclames": { "$in": &fuuids } };
        let collection_versions = mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;
        let mut cursor = collection_versions.find(filtre).await?;
        while let Some(row) = cursor.next().await {
            original_fuuids.push(row?.fuuid);
        }

        // Filter by user from fichiers_rep
        let filtre = doc!{"fuuids_versions": {"$in": &original_fuuids}, "user_id": &user_id};
        let collection_reps = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
        let mut cursor = collection_reps.find(filtre).await?;
        let mut verified_fuuids = HashSet::new();
        while let Some(row) = cursor.next().await {
            if let Some(fuuids) = row?.fuuids_versions {
                for fuuid in fuuids {
                    if original_fuuids.contains(&fuuid) {
                        verified_fuuids.insert(fuuid);
                    }
                }
            }
        }

        verified_fuuids
    };

    debug!("verifier_acces_usager_media Original fuuids = {:?}", verified_fuuids);

    // Get all media available to the user for these files
    let allowable_fuuids = {
        let mut fuuids_acces = HashSet::new();
        let fuuids_list: Vec<&String> = verified_fuuids.iter().collect();
        let filtre = doc! {
            FIELD_FUUID: { "$in": fuuids_list },
            FIELD_USER_ID: user_id,
        };
        let collection = mongo.get_collection_typed::<MediaOwnedRow>(NOM_COLLECTION_MEDIA)?;
        let mut curseur = collection.find(filtre).await?;
        while let Some(row) = curseur.next().await {
            let doc_map = row?;
            fuuids_acces.insert(doc_map.fuuid);
            if let Some(images) = doc_map.images {
                fuuids_acces.extend(images.into_values().map(|x| x.hachage));
            }
            if let Some(video) = doc_map.video {
                fuuids_acces.extend(video.into_values().map(|x| x.fuuid_video));
            }
        }

        // Include all original fuuids found in the fichiers_rep table by user.
        fuuids_acces.extend(verified_fuuids);

        fuuids_acces
    };

    debug!("verifier_acces_usager_media Allowable : {:?}", allowable_fuuids);

    // Join the set of fuuids requested to the allowable fuuids
    let hashset_requete = HashSet::from_iter(fuuids.into_iter().map(|fuuid| fuuid.to_string()));
    let resultat: Vec<&String> = allowable_fuuids.intersection(&hashset_requete).collect();

    debug!("verifier_acces_usager_media Intersection : {:?}", resultat);

    // String to_owned
    Ok(resultat.into_iter().map(|s| s.to_string()).collect())
}

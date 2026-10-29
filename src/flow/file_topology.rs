/// Module that handles interactions with CoreTopology and the file controler.
/// This ensures files are claimed regularly to avoid being deleted. It also handles "visits" from
/// the filecontroler for each file when they are emitted.

use crate::external::mongo::*;
use crate::external::mongo_struct::NodeFichierRepOwned;
use crate::flow::maintenance::{RequeteGetVisitesFuuidsResponse, verifier_visites_topologies};
use crate::models::TypeNode;
use millegrilles_common_rust::bson;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::constantes::{DOMAINE_TOPOLOGIE, Securite};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::jwt_simple::prelude::{Deserialize, Serialize};
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::mongo_serde::{map_chrono_datetime_as_bson_datetime, option_chrono_04_datetime};
use millegrilles_common_rust::tokio;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, info, warn};
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use std::collections::HashSet;

#[derive(Deserialize)]
struct FuuidReclamesRow {
    fuuids_reclames: Vec<String>
}

const VISIT_BATCH_SIZE: usize = 1000;

/// Claims all required files on filehosts. Prevents them from being deleted.
pub async fn claim_all_files<M>(mongo: &M, outbound: &MessageOutboundFacade) -> Result<(), CommonError> where M: MongoDaoTyped {
    info!("claim_all_files Claim fuuids");

    // Faire une reclamation des fichiers regulierement (tous les jours) pour eviter qu'ils soient
    // consideres comme orphelins (et supprimes).
    let filtre = doc!{
        "tuuids.0": {"$exists": true},  // Check if at least one tuuid is linked (means not deleted)
    };

    let collection_versions = mongo.get_collection_typed::<FuuidReclamesRow>(NOM_COLLECTION_VERSIONS)?;
    let mut cursor = collection_versions
        .find(filtre)
        .projection(doc!{"fuuids_reclames": 1})
        .await?;
    let mut visits = Vec::with_capacity(VISIT_BATCH_SIZE);
    let mut batch_no = 0;
    while let Some(row) = cursor.next().await {
        let row = cursor.deserialize_current()?;
        visits.extend(row.fuuids_reclames);

        if visits.len() >= VISIT_BATCH_SIZE {
            let mut reponse = claim_files(outbound, batch_no, false, &visits).await?;
            for _ in 0..3 {
                if reponse.ok {
                    break;  // Success
                }
                // Wait 5 seconds
                warn!("claim_all_files Error receiving response for batch of claims, retrying: Error: {:?}", reponse.err);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                reponse = claim_files(outbound, batch_no, false, &visits).await?;
            }

            if ! reponse.ok {
                Err(format!("Error claiming files, aborting: {:?}", reponse.err))?;
            }

            debug!("claim_all_files Batch {} of {} fuuids claimed", batch_no, visits.len());
            visits.clear();
            batch_no += 1;
        }
    }

    // Last batch
    let mut reponse = claim_files(outbound, batch_no, true, &visits).await?;
    for _ in 0..3 {
        if reponse.ok {
            break;  // Success
        }
        // Wait 5 seconds
        warn!("claim_all_files Error receiving response for last batch of claims, retrying: Error: {:?}", reponse.err);
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        reponse = claim_files(outbound, batch_no, true, &visits).await?;
    }
    if ! reponse.ok {
        warn!("claim_all_files Error on last batch of claims: {:?}", reponse.err);
    } else {
        info!("claim_all_files Done with {} batches completed.", batch_no + 1);
    }

    Ok(())
}

#[derive(Serialize)]
struct RequeteFuuidsVisites<'a> {
    fuuids: &'a Vec<String>,
    batch_no: Option<usize>,
    done: Option<bool>,
}

async fn claim_files(
    outbound: &MessageOutboundFacade,
    batch_no: usize,
    done: bool,
    fuuids: &Vec<String>
) -> Result<RequeteGetVisitesFuuidsResponse, CommonError> {
    let requete = RequeteFuuidsVisites { fuuids, batch_no: Some(batch_no), done: Some(done) };

    let routage = RoutageMessageAction::builder(
        DOMAINE_TOPOLOGIE, "claimFiles", vec![Securite::L3Protege]).build();

    let response = match outbound.send_command(routage, requete).await? {
        Some(response) => response,
        None => return Err(CommonError::Str("claim_files: No response received"))
    };

    match response.is_err()? {
        (true, err) => Err(CommonError::String(format!("claim_files: Error claiming files: {:?}", err))),
        _ => Ok(response.message.deserialize()?)
    }
}

#[derive(Serialize, Deserialize)]
pub struct VisitWorkRow {
    pub fuuid: String,
    pub filehost_id: Option<String>,
    #[serde(with="option_chrono_04_datetime")]
    pub visit_time: Option<DateTime<Utc>>,
}

pub async fn receive_visits(mongo: &dyn MongoDao, command: RequeteGetVisitesFuuidsResponse) -> Result<(), CommonError> {
    let collection = mongo.get_collection(NOM_COLLECTION_TEMP_VISITS)?;
    if let Some(visits) = command.visits {
        debug!("Received {} visit entries", visits.len());
        let mut batch = Vec::with_capacity(visits.len());
        for visit in visits {
            for (filehost_id, timestamp) in visit.visits {
                let visit_date = DateTime::from_timestamp(timestamp, 0).expect("visit_date from_timestamp");
                let row = VisitWorkRow { fuuid: visit.fuuid.clone(), filehost_id: Some(filehost_id), visit_time: Some(visit_date) };
                batch.push(bson::serialize_to_document(&row)?);
            }
        }
        if ! batch.is_empty() {
            collection.insert_many(batch).await?;
        }
    }

    if let Some(unknowns) = command.unknown {
        debug!("Received {} unknwown fuuid claims responses", unknowns.len());
        let mut batch = Vec::with_capacity(unknowns.len());
        for fuuid in unknowns {
            let row = VisitWorkRow { fuuid, filehost_id: None, visit_time: None };
            batch.push(bson::serialize_to_document(&row)?);
        }
        if ! batch.is_empty() {
            collection.insert_many(batch).await?;
        }
    }

    Ok(())
}

pub async fn claim_files_under_deleted_cuuids<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    user_id: &String,
    directory_tuuids: &Vec<String>
) -> Result<Vec<String>, CommonError> where M: MongoDaoTyped {
    let type_node_fichier: &str = TypeNode::Fichier.to_str();

    let mut fuuids = HashSet::new();

    let collection =
        mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;

    let filtre = doc! {
        CHAMP_PATH_CUUIDS: {"$in": directory_tuuids},
        CHAMP_USER_ID: &user_id,
        CHAMP_TYPE_NODE: type_node_fichier,
        CHAMP_SUPPRIME: true,
        CHAMP_SUPPRIME_INDIRECT: true,
    };
    let mut cursor = collection.find(filtre).await?;
    while let Some(row) = cursor.next().await {
        let row = cursor.deserialize_current()?;
        if let Some(fuuids_versions) = row.fuuids_versions {
            fuuids.extend(fuuids_versions);
        }
    }

    // Issue a claim command for all fuuids to restore to CoreTopologie. Ensure none have been deleted.
    let fuuids: Vec<String> = fuuids.into_iter().collect();
    for fuuid_batch in fuuids.chunks(100) {
        debug!("Check batch of fuuids to restore: {:?}", fuuid_batch);
        claim_files_by_fuuids(outbound, &fuuid_batch.to_vec()).await?;
    }

    Ok(fuuids)
}

pub async fn claim_files_by_fuuids(outbound: &MessageOutboundFacade, fuuid_batch: &Vec<String>) -> Result<(), CommonError> {
    let fuuid_batch_ref = fuuid_batch.iter().map(|v| v.as_str()).collect();
    match verifier_visites_topologies(outbound, &fuuid_batch_ref).await {
        Ok(response) => {
            if response.ok != true {
                Err(format!("commandes.command_recycle_items_v3 Error verifying presence of files - server error: {:?}", response.err))?
            }
            // Claim completed. This will prevent the files from being deleted for a while.
            // Check that all files were found. Fail if at least 1 file was already permanently deleted.
            debug!("Batch size: {}", fuuid_batch.len());
            if let Some(v) = response.visits {
                debug!("Response visits: {}", v.len());
                if v.len() < fuuid_batch.len() {
                    Err(format!("commandes.command_recycle_items_v3 Error verifying presence of files (A) - some files have been permanently deleted: {:?}", response.err))?
                }
            }
            if let Some(unknown) = response.unknown {
                debug!("Response unknown: {}", unknown.len());
                if unknown.len() > 0 {
                    Err(format!("commandes.command_recycle_items_v3 Error verifying presence of files (B) - some files have been permanently deleted: {:?}", response.err))?;
                }
            }
        }
        Err(e) => {
            Err(format!("commandes.command_recycle_items_v3 Error verifying presence of files - timeout or other issue: {:?}", e))?;
        }
    }
    Ok(())
}

pub async fn claim_files_by_deleted_tuuids<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    user_id: &str,
    tuuid_batch: &Vec<String>
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let type_node_fichier: &str = TypeNode::Fichier.to_str();
    let mut fuuids = HashSet::new();

    let collection =
        mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;

    let filtre = doc! {
        FIELD_TUUID: { "$in": tuuid_batch },
        CHAMP_USER_ID: &user_id,
        CHAMP_TYPE_NODE: type_node_fichier,
        CHAMP_SUPPRIME: true,
        CHAMP_SUPPRIME_INDIRECT: true,
    };
    let mut cursor = collection.find(filtre).await?;
    while let Some(row) = cursor.next().await {
        let row = cursor.deserialize_current()?;
        if let Some(fuuids_versions) = row.fuuids_versions {
            fuuids.extend(fuuids_versions);
        }
    }

    // Issue a claim command for all fuuids to restore to CoreTopologie. Ensure none have been deleted.
    let fuuids: Vec<String> = fuuids.into_iter().collect();
    for fuuid_batch in fuuids.chunks(100) {
        debug!("Check batch of fuuids to restore: {:?}", fuuid_batch);
        claim_files_by_fuuids(outbound, &fuuid_batch.to_vec()).await?;
    }

    Ok(())
}
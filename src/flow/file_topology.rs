use crate::constants::COMMAND_CLAIM_FILEHOST_VISITS;
/// Module that handles interactions with CoreTopology and the file controler.
/// This ensures files are claimed regularly to avoid being deleted. It also handles "visits" from
/// the filecontroler for each file when they are emitted.

use crate::external::mongo::*;
use crate::external::mongo_struct::NodeFichierRepOwned;
use crate::flow::file_events::{DelayedEventEmitterService, declencher_traitement_nouveau_fuuid};
use crate::models::TypeNode;
use millegrilles_common_rust::bson;
use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use millegrilles_common_rust::constantes::{DOMAINE_TOPOLOGIE, Securite};
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::jwt_simple::prelude::{Deserialize, Serialize};
use millegrilles_common_rust::mongo_dao::{ChampIndex, IndexOptions, MongoDao, MongoDaoTyped};
use millegrilles_common_rust::mongo_serde::{map_chrono_datetime_as_bson_datetime, option_chrono_04_datetime};
use millegrilles_common_rust::mongodb::ClientSession;
use millegrilles_common_rust::mongodb::options::Hint;
use millegrilles_common_rust::tokio;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use std::collections::{HashMap, HashSet};

pub async fn maintain_visits_claims<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    trigger_date: Option<&DateTime<Utc>>
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let (run_visits, run_claims, run_newfiles, run_deleted_files, run_quotas) = match trigger_date {
        Some(date) => {
            let hours = date.hour();
            let minutes = date.minute();

            let run_visits = minutes % 6 == 5;
            let run_claims = hours % 8 == 1 && minutes == 14;
            let run_newfiles = minutes % 6 == 0;
            let run_deleted_files = hours % 6 == 1 && minutes == 6;
            let run_quotas = hours % 3 == 1 && minutes == 14;

            (run_visits, run_claims, run_newfiles, run_deleted_files, run_quotas)
        },
        None => (true, true, true, true, true)
    };

    if run_visits {
        if let Err(e) = process_visits(mongo).await {
            error!("process_visits Error processing filehost visits log: {:?}", e);
        }
    }

    if run_claims {
        if let Err(e) = claim_all_files(mongo, outbound).await {
            error!("process_visits Error processing filehost visits log: {:?}", e);
        }
    }

    if run_newfiles {
        if let Err(e) = reclamer_fichiers(mongo, outbound, delayed_event_emitter_service).await {
            error!("process_visits Error processing filehost visits log: {:?}", e);
        }
    }

    if run_deleted_files {
        // TODO maintain_deleted_files
    }

    if run_quotas {
        // TODO calculer_quotas
    }

    Ok(())
}

/// Processes the entries received in the temp visits table.
pub async fn process_visits<M>(
    mongo: &M
) -> Result<(), CommonError>
where M: MongoDaoTyped
{
    // Check if we have some records to process
    let collection_temp_visits = mongo.get_collection(NOM_COLLECTION_TEMP_VISITS)?;
    {
        let result = collection_temp_visits.find_one(doc!{}).await?;
        if result.is_none() {
            debug!("process_visits No entries to process");
            return Ok(())
        }  // Nothing to do
    }

    // Take over the collection, rename it to _WORK
    let collection_visits_work_name = format!("{}_WORK", NOM_COLLECTION_TEMP_VISITS);
    mongo.rename_collection(NOM_COLLECTION_TEMP_VISITS, &collection_visits_work_name, true).await?;

    let collection_work = mongo.get_collection_typed::<VisitWorkRow>(collection_visits_work_name.as_str())?;
    // Create an index to facilitate grouping by fuuid
    {
        let options_fuuids = IndexOptions { nom_index: Some(format!("fuuids")), unique: false };
        let champs_index_fuuids_version = vec!( ChampIndex { nom_champ: String::from("fuuid"), direction: 1 } );
        mongo.create_index(collection_visits_work_name.as_str(), champs_index_fuuids_version, Some(options_fuuids)).await?;
    }

    info!("process_visits Processing visit batches START");

    // Process the unknown visits - this empties the visites object.
    let collection_versions = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let filtre = doc!{"visit_time": null};
    let mut cursor = collection_work.find(filtre).await?;
    let mut removed_visits = Vec::new();
    let ops = doc!{
        "$set": {CHAMP_VISITES: {}},
        "$currentDate": {FIELD_DATE_MODIFIED: true, "last_visit_verification": true},
    };
    while cursor.advance().await? {
        let row = cursor.deserialize_current()?;
        removed_visits.push(row.fuuid);
        if removed_visits.len() > 50 {
            let filtre = doc!{"fuuid": {"$in": &removed_visits}};
            collection_versions.update_many(filtre, ops.clone()).await?;
            removed_visits.clear();
        }
    }
    if ! removed_visits.is_empty() {
        let filtre = doc!{"fuuid": {"$in": removed_visits}};
        collection_versions.update_many(filtre, ops.clone()).await?;
    }

    // Aggregation pipeline to process the visits
    let pipeline = vec![
        doc!{"$sort": {CHAMP_FUUID: 1}},                    // Should use the index
        doc!{"$match": {"visit_time": {"$exists": true}}},  // Filter out unknown fuuid entries

        doc!{"$addFields": {"obj": {"k": "$filehost_id", "v": "$visit_time"}}},
        doc!{"$group": {"_id": "$fuuid", "items": {"$push": "$obj"}}},
        doc!{"$addFields": {
            "fuuid": "$_id",
            CHAMP_VISITES: {"$arrayToObject": "$items"},
            FIELD_DATE_MODIFIED: "$$NOW",            // Required for changes to get picked-up
            "last_visit_verification": "$$NOW",
        }},
        doc!{"$unset": ["_id", "items"]},
        doc!{"$merge": {
            "into": NOM_COLLECTION_VERSIONS,
            "on": "fuuid",
            "whenNotMatched": "discard",
        }},
    ];
    collection_work.aggregate(pipeline).await?;

    info!("process_visits Processing visit batches DONE");

    // Done with the work collection, cleanup.
    collection_work.drop().await?;

    Ok(())
}


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
    // let fuuid_batch_ref = fuuid_batch.iter().map(|v| v.as_str()).collect();
    match verifier_visites_topologies(outbound, fuuid_batch).await {
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

#[derive(Deserialize)]
pub struct RowFuuidVisit {
    pub fuuid: String,
    pub visits: HashMap<String, i64>,
}

#[derive(Deserialize)]
pub struct RequeteGetVisitesFuuidsResponse {
    pub ok: bool,
    pub err: Option<String>,
    pub visits: Option<Vec<RowFuuidVisit>>,
    pub unknown: Option<Vec<String>>
}

pub async fn verifier_visites_topologies(
    outbound: &MessageOutboundFacade,
    fuuids: &Vec<String>
) -> Result<RequeteGetVisitesFuuidsResponse, CommonError> {
    let request = RequeteFuuidsVisites { fuuids, batch_no: None, done: None };

    let routing = RoutageMessageAction::builder(
        DOMAINE_TOPOLOGIE,
        COMMAND_CLAIM_FILEHOST_VISITS,
        vec![Securite::L3Protege]
    ).build();
    match outbound.send_command(routing, request).await? {
        Some(response) => {
            if let (true, err) = response.is_err()? {
                Err(CommonError::String(format!("Error on command claimAndFilehostVisits: {:?}", err)))
            } else {
                Ok(response.message.deserialize::<RequeteGetVisitesFuuidsResponse>()?)
            }
        },
        None => Err(CommonError::Str("No response received for claimAndFilehostVisits command"))
    }
}

pub async fn reclamer_fichiers<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let mut session = mongo.get_session().await?;
    session.start_transaction().await?;
    match verifier_visites_nouvelles(mongo, outbound, delayed_event_emitter_service, &mut session).await {
        Ok(()) => {
            session.commit_transaction().await?;
            Ok(())
        },
        Err(e) => {
            session.abort_transaction().await?;
            Err(e)
        }
    }
}

async fn verifier_visites_nouvelles<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    delayed_event_emitter_service: &DelayedEventEmitterService,
    session: &mut ClientSession
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let now = Utc::now();

    // Faire une verification des fichiers qui sont encore "nouveaux" avec une date de
    // creation recente (< 30 minutes).
    let delai_expiration = Duration::minutes(30);
    let expiration = now - delai_expiration;

    debug!("verifier_visites_nouvelles Verifier nouveaux (visites.nouveau, epoch {} et plus recent)", expiration);

    // Cleanup of all expired entries
    let collection_versions = mongo.get_collection_typed::<FuuidReclamesRow>(NOM_COLLECTION_VERSIONS)?;
    let filtre_expire = doc!{"visites.nouveau": {"$lt": expiration}};
    let ops = doc!{"$unset": {"visites.nouveau": true}, "$currentDate": { FIELD_DATE_MODIFIED: true }};
    collection_versions
        .update_many(filtre_expire, ops)
        .session(&mut *session)
        .await?;

    let filtre = doc!{
        "visites.nouveau": {"$gte": expiration},
        "tuuids.0": {"$exists": true},  // Check if at least one tuuid is linked (means not deleted)
    };

    let mut curseur = collection_versions
        .find(filtre)
        .limit(VISIT_BATCH_SIZE as i64)
        .hint(Hint::Name("last_visits".to_string()))  // Sorts by last_visit ASC
        .projection(doc!{"fuuids_reclames": 1})
        .session(&mut *session)
        .await?;

    let mut visits = Vec::with_capacity(VISIT_BATCH_SIZE);
    while let Some(row) = curseur.next(session).await {
        let fuuids = row?.fuuids_reclames;
        visits.extend(fuuids);
        if visits.len() > VISIT_BATCH_SIZE {
            // Already 1000 items, break and continue with another batch later
            break
        }
    }

    debug!("verifier_visites_nouvelles Verifier {} fuuids", visits.len());

    if visits.len() == 0 {
        return Ok(())  // Nothing to do
    }

    // The following code does not use transactions, just commit the work
    session.commit_transaction().await?;

    let reponse = verifier_visites_topologies(outbound, &visits).await?;
    if let Some(visites) = reponse.visits {
        debug!("verifier_visites_nouvelles Visite {} nouveaux fuuids", visites.len());
        for item in visites {
            // Emettre evenement consigne pour indiquer que le fichier n'est plus nouveau
            sauvegarder_visites(mongo, item.fuuid.as_str(), &item.visits, session).await?;
            let filehosts: Vec<String> = item.visits.keys().map(|k| k.to_string()).collect();
            declencher_traitement_nouveau_fuuid(
                mongo,
                outbound,
                delayed_event_emitter_service,
                item.fuuid.as_str(),
                &filehosts,
            ).await?;
        }
    }

    // Need to restart a transaction to finish
    session.start_transaction().await?;
    if let Some(unknown) = reponse.unknown {
        debug!("verifier_visites_nouvelles {} fuuids inconnus", unknown.len());
        sauvegarder_fuuid_inconnu(mongo, &unknown, session).await?;
    }

    Ok(())
}

async fn sauvegarder_visites<M>(
    mongo: &M,
    fuuid: &str,
    visites: &HashMap<String, i64>,
    session: &mut ClientSession
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let filtre = doc!{"fuuid": fuuid};

    let mut visits_date = doc!{};
    for (k,v) in visites {
        let date_time = match DateTime::from_timestamp(*v, 0) {
            Some(inner) => inner,
            None => Err("sauvegarder_visites Invalid visit date")?
        };
        visits_date.insert(k.to_string(), date_time);
    }

    let ops = doc!{
        "$set": { "visites": visits_date },
        "$currentDate": {
            FIELD_DATE_MODIFIED: true,
            CONST_FIELD_LAST_VISIT_VERIFICATION: true
        },
    };

    debug!("sauvegarder_visites ops {:?}", ops);

    let collection = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    collection
        .update_one(filtre, ops)
        .session(&mut *session)
        .await?;

    Ok(())
}

async fn sauvegarder_fuuid_inconnu<M>(
    middleware: &M,
    fuuids: &Vec<String>,
    session: &mut ClientSession
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let filtre = doc!{"fuuid": {"$in": fuuids}};
    let ops = doc!{
        "$currentDate": {
            FIELD_DATE_MODIFIED: true,
            CONST_FIELD_LAST_VISIT_VERIFICATION: true
        },
    };

    let collection = middleware.get_collection(NOM_COLLECTION_VERSIONS)?;
    collection
        .update_many(filtre, ops)
        .session(&mut *session)
        .await?;

    Ok(())
}

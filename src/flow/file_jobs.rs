use std::collections::{HashMap, HashSet};
use millegrilles_common_rust::{bson, serde_json};
use millegrilles_common_rust::bson::{doc, Document};
use millegrilles_common_rust::chrono::{DateTime, Duration, Timelike, Utc};
use millegrilles_common_rust::constantes::*;
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::jwt_simple::prelude::{Deserialize, Serialize};
use millegrilles_common_rust::mongo_dao::{MongoDao, MongoDaoTyped};
use millegrilles_common_rust::uuid::Uuid;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::mongodb::options::ReturnDocument;
use millegrilles_common_rust::tracing::{debug, error, info, warn};
use millegrilles_common_rust::bson::serde_helpers::datetime::FromChrono04DateTime;
use millegrilles_common_rust::common_messages::ResponseRequestDechiffrageV2Cle;
use millegrilles_common_rust::mongo_serde::option_chrono_04_datetime;
use millegrilles_common_rust::mongodb::{ClientSession, Collection};
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::v3::models::ErrorMessage;
use millegrilles_common_rust::v3::TransactionService;
use crate::constants::*;
use crate::external::mongo::*;
use crate::external::mongo_struct::{CompleteFileRow, MediaOwnedRow, NodeFichierRepOwned, NodeFichierVersionOwned};
use crate::external::mq::*;
use crate::flow::file_topology::sauvegarder_visites;
use crate::flow::keys::convert_decrypted_key_into_v2;
use crate::flow::request_models::FileCommentResponse;
use crate::flow::transaction_models::*;

const CONST_MAX_RETRY: i32 = 4;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackgroundJobParams {
    #[serde(skip_serializing_if="Option::is_none")]
    pub defaults: Option<bool>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub thumbnails: Option<bool>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub mimetype: Option<String>,
    #[serde(rename="codecVideo", skip_serializing_if="Option::is_none")]
    pub codec_video: Option<String>,
    #[serde(rename="codecAudio", skip_serializing_if="Option::is_none")]
    pub codec_audio: Option<String>,
    #[serde(rename="resolutionVideo", skip_serializing_if="Option::is_none")]
    pub resolution_video: Option<u32>,
    #[serde(rename="qualityVideo", skip_serializing_if="Option::is_none")]
    pub quality_video: Option<i32>,
    #[serde(rename="bitrateVideo", skip_serializing_if="Option::is_none")]
    pub bitrate_video: Option<u32>,
    #[serde(rename="bitrateAudio", skip_serializing_if="Option::is_none")]
    pub bitrate_audio: Option<u32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub preset: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub audio_stream_idx: Option<i32>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub subtitle_stream_idx: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackgroundJob {
    pub job_id: String,

    // Parametres de la job
    pub tuuid: String,
    pub fuuid: Option<String>,
    pub mimetype: String,
    pub filehost_ids: Vec<String>,
    pub params: Option<BackgroundJobParams>,

    // Valeurs pour video (progress update) et index (access rights)
    #[serde(skip_serializing_if="Option::is_none")]
    pub user_id: Option<String>,

    // Dechiffrage fichier (fuuid)
    pub cle_id: String,
    pub format: String,
    pub nonce: String,

    // Etat de la job
    pub etat: i32,
    #[serde(with="FromChrono04DateTime")]
    pub modified: DateTime<Utc>,
    #[serde(default, with="option_chrono_04_datetime")]
    pub date_maj: Option<DateTime<Utc>>,
    pub retry: i32,
    #[serde(skip_serializing_if="Option::is_none")]
    pub pct_progres: Option<i32>,

    // Business key, combined with tuuid to make a unique job value
    pub cle_conversion: Option<String>,
}

impl BackgroundJob {
    pub fn new(
        tuuid: &str,
        fuuid: &str,
        mimetype: &str,
        filehost_ids: &Vec<&str>,
        cle_id: &str,
        format: &str,
        nonce: &str
    ) -> BackgroundJob {
        let job_id = Uuid::new_v4();  // Generate random identifier
        Self {
            job_id: job_id.to_string(),
            tuuid: tuuid.to_string(),
            fuuid: Some(fuuid.to_string()),
            mimetype: mimetype.to_string(),
            filehost_ids: filehost_ids.iter().map(|id| id.to_string()).collect(),
            params: None,
            user_id: None,
            cle_id: cle_id.to_string(),
            format: format.to_string(),
            nonce: nonce.to_string(),
            etat: VIDEO_CONVERSION_ETAT_PENDING,
            modified: Utc::now(),
            date_maj: None,
            retry: 0,
            pct_progres: None,
            cle_conversion: None,
        }
    }

    pub fn new_index(
        tuuid: &str,
        fuuid: Option<&str>,
        user_id: &str,
        mimetype: &str,
        filehost_ids: &Vec<&str>,
        cle_id: &str,
        format: &str,
        nonce: &str
    ) -> BackgroundJob {
        let job_id = Uuid::new_v4();  // Generate random identifier
        Self {
            job_id: job_id.to_string(),
            tuuid: tuuid.to_string(),
            fuuid: match fuuid {Some(inner) => Some(inner.to_string()), None => None},
            mimetype: mimetype.to_string(),
            filehost_ids: filehost_ids.iter().map(|id| id.to_string()).collect(),
            params: None,
            user_id: Some(user_id.to_string()),
            cle_id: cle_id.to_string(),
            format: format.to_string(),
            nonce: nonce.to_string(),
            etat: VIDEO_CONVERSION_ETAT_PENDING,
            modified: Utc::now(),
            date_maj: None,
            retry: 0,
            pct_progres: None,
            cle_conversion: None,
        }
    }
}

pub async fn sauvegarder_job_video<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    job: &BackgroundJob
) -> Result<Option<BackgroundJob>, CommonError> where M: MongoDaoTyped {
    let mimetype = job.mimetype.as_str();
    if job_video_supportee(mimetype) {
        Ok(Some(sauvegarder_job(mongo, outbound, job, NOM_COLLECTION_VIDEO_JOBS, "media", "processVideo").await?))
    } else {
        Ok(None)
    }
}

pub async fn sauvegarder_job<'a, M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    job: &BackgroundJob,
    nom_collection: &str,
    domain: &str,
    action_trigger: &str,
) -> Result<BackgroundJob, CommonError> where M: MongoDaoTyped {
    let collection = mongo.get_collection_typed::<BackgroundJob>(nom_collection)?;

    // Verifier si une job existe deja pour le fichier represente
    let cle_conversion = match job.params.as_ref() {
        Some(params) => {
            if Some(true) == params.defaults {
                "defaults".to_string()
            } else {
                // Generate unique key from params
                format!("{:?};{:?};{:?};{:?};{:?};{:?}", params.mimetype, params.codec_video,
                        params.resolution_video, params.bitrate_audio, params.audio_stream_idx,
                        params.subtitle_stream_idx)
            }
        },
        None => "defaults".to_string()
    };
    let filtre = doc!{"tuuid": &job.tuuid, "cle_conversion": &cle_conversion};
    let ops = doc! {
        "$addToSet": {"filehost_ids": {"$each": &job.filehost_ids}},  // Merge the filehost_ids if appropriate
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };
    let updated_job = match collection
        .find_one_and_update(filtre, ops)
        .return_document(ReturnDocument::After)
        .await?
    {
        Some(inner) => inner,
        None => {
            debug!("sauvegarder_job No job updated, returning cloned job");
            let mut job_copy = job.clone();
            job_copy.cle_conversion = Some(cle_conversion);
            collection.insert_one(job_copy).await?;
            job.clone()
        }
    };

    // Emettre job pour traitement.
    emettre_processing_trigger(outbound, &updated_job, domain, action_trigger).await;

    Ok(updated_job)
}

fn job_video_supportee(mimetype: &str) -> bool {
    is_mimetype_video(mimetype)
}

pub fn is_mimetype_video(mimetype: &str) -> bool {
    const VIDEO_START: &'static str = "video/";
    const VIDEO_MIMETYPES: [&'static str; 5] = [
        "application/vnd.rn-realmedia",
        "application/vnd.rn-realplayer",
        "application/x-mplayer2",
        "application/x-shockwave-flash",
        "application/vnd.ms-asf"
    ];

    if mimetype.starts_with(VIDEO_START) {
        true
    } else {
        for mt in VIDEO_MIMETYPES {
            if mt == mimetype {
                return true
            }
        }
        false
    }
}

#[derive(Serialize)]
pub struct JobTrigger<'a> {
    pub job_id: &'a str,
    pub tuuid: &'a str,
    pub fuuid: Option<&'a str>,
    pub mimetype: &'a str,
    pub filehost_ids: &'a Vec<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub params: Option<&'a BackgroundJobParams>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub user_id: Option<&'a str>,
    pub cle_id: &'a str,
    pub format: &'a str,
    pub nonce: &'a str,
    #[serde(skip_serializing_if="Option::is_none")]
    pub metadata: Option<&'a DataChiffre>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub path_cuuids: Option<Vec<String>>,
}

impl<'a> From<&'a BackgroundJob> for JobTrigger<'a> {
    fn from(value: &'a BackgroundJob) -> JobTrigger<'a> {
        Self {
            job_id: value.job_id.as_str(),
            tuuid: value.tuuid.as_str(),
            fuuid: match &value.fuuid {Some(inner)=>Some(inner), None=>None},
            mimetype: value.mimetype.as_str(),
            filehost_ids: &value.filehost_ids,
            params: match &value.params {Some(inner)=>Some(inner), None=>None},
            user_id: match value.user_id.as_ref() {Some(inner)=>Some(inner.as_str()), None=>None},
            cle_id: value.cle_id.as_str(),
            format: value.format.as_str(),
            nonce: value.nonce.as_str(),
            metadata: None,
            path_cuuids: None,
        }
    }
}

pub async fn emettre_processing_trigger<'a, T>(outbound: &MessageOutboundFacade, trigger: T, domain: &str, action: &str)
where T: Into<JobTrigger<'a>> {
    // let trigger = JobTrigger::from(background_job);
    let trigger = trigger.into();
    if trigger.filehost_ids.is_empty() {
        let routing = RoutageMessageAction::builder(domain, action, vec![Securite::L2Prive])
            .blocking(false)
            .correlation_id("DUMMY")  // Required for commands, not generated on non-blocking
            .build();
        if let Err(e) = outbound.send_command(routing, &trigger).await {
            error!("emettre_processing_trigger Erreur emission trigger commande.{}.{} : {:?}", domain, action, e);
        }
    } else {
        for filehost_id in trigger.filehost_ids {
            let routage = RoutageMessageAction::builder(domain, action, vec![Securite::L2Prive])
                .partition(filehost_id)
                .blocking(false)
                .correlation_id("DUMMY")  // Required for commands, not generated on non-blocking
                .build();
            if let Err(e) = outbound.send_command(routage, &trigger).await {
                error!("emettre_processing_trigger Erreur emission trigger commande.{}.{}.{} : {:?}", domain, filehost_id, action, e);
            }
        }
    }
}

#[derive(Deserialize)]
pub struct RequestLeaseForBatchOperation {
    pub batch_size: Option<usize>,
    filehost_id: Option<String>,
}


#[derive(Serialize, Deserialize)]
pub struct LeaseVersionMedia {
    version: NodeFichierVersionOwned,
    media: Option<MediaOwnedRow>,
}

#[derive(Serialize)]
pub struct VersionLeasesResponse {
    ok: bool,
    leases: Vec<LeaseVersionMedia>,
    secret_keys: Vec<ResponseRequestDechiffrageV2Cle>,
}

pub async fn get_batch_of_fichiersversion_leases<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    request: RequestLeaseForBatchOperation,
    borrower: &str,
)  -> Result<Option<VersionLeasesResponse>, CommonError> where M: MongoDaoTyped {
    let expiry = Utc::now() - Duration::seconds(300);
    let batch_size = request.batch_size.unwrap_or(5);

    // Iterate through files that have a flag_rag == false or undefined.
    let filter = match request.filehost_id.as_ref() {
        Some(filehost_id) => {
            doc!{
                "tuuids.0": { "$exists": true },
                format!("visites.{}", filehost_id): {"$exists": true},
                "$or": [
                    { borrower: {"$exists": false} },
                    { borrower: false },
                ]
            }
        },
        None => doc!{
            "tuuids.0": { "$exists": true },
            "$or": [
                {borrower: {"$exists": false}},
                {borrower: false},
            ]
        }
    };

    Ok(lease_batch_fichiersversion(mongo, outbound, &expiry, borrower, filter, batch_size).await?)
}

/// Lease a batch of files based on a FichiersRep filtre.
pub async fn lease_batch_fichiersversion<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    expiry: &DateTime<Utc>,
    borrower: &str,
    filter: Document,
    batch_size: usize,
) -> Result<Option<VersionLeasesResponse>, CommonError> where M: MongoDaoTyped {
    let collection_versions = mongo.get_collection_typed::<NodeFichierVersionOwned>(NOM_COLLECTION_VERSIONS)?;

    let mut pipeline = vec![doc!{"$match": filter}];

    pipeline.push(doc!{"$replaceRoot": {"newRoot": {"version": "$$ROOT"}}});

    // Join media information
    pipeline.push(doc!{"$lookup": {
            "from": NOM_COLLECTION_MEDIA,
            "localField": "version.fuuid",
            "foreignField": "fuuid",
            // "limit": 1,
            "as": "medias",
        }}
    );
    pipeline.push(doc!{"$addFields": {
        "media": {"$arrayElemAt": ["$medias", 0]},
    }});
    pipeline.push(doc!{"$unset": "medias"});

    let mut cursor = collection_versions.aggregate(pipeline).await?;

    // let mut cursor = collection_versions.find(filtre, None).await?;
    let mut leases = Vec::with_capacity(batch_size);
    while cursor.advance().await? {
        let file = cursor.deserialize_current()?;
        let file: LeaseVersionMedia = bson::deserialize_from_document(file)?;

        // Attempt a lease on the file
        if lease_file_version(mongo, &file.version.fuuid, borrower, expiry).await? {
            leases.push(file);
        }

        if leases.len() >= batch_size {
            break
        }
    }

    if leases.len() > 0 {
        info!("lease_batch_fichiersrep Fetching decryption keys for {} leases for borrower {}", leases.len(), borrower);
        // Get all decrypted keys for the files
        let mut cle_ids = HashSet::with_capacity(leases.len());
        for lease in &leases {
            if let Some(cle_id) = lease.version.cle_id.as_ref() {
                cle_ids.insert(cle_id.as_str());
            } else {
                cle_ids.insert(&lease.version.fuuid.as_str());  // Legacy
            }
        }
        let secret_keys = get_decrypted_keys_by_id(outbound, &cle_ids.into_iter().collect()).await?;
        Ok(Some(VersionLeasesResponse { ok: true, leases, secret_keys }))
    } else {
        Ok(None)
    }
}

pub async fn get_decrypted_keys_by_id(
    outbound: &MessageOutboundFacade,
    keys: &Vec<&str>
) -> Result<Vec<ResponseRequestDechiffrageV2Cle>, CommonError> {
    let key_ids: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    let keys = outbound.get_keys(DOMAIN_NAME, key_ids, None).await?;
    let mut secret_keys = Vec::with_capacity(keys.len());
    for k in keys {
        secret_keys.push(convert_decrypted_key_into_v2(k)?);
    }
    Ok(secret_keys)
}

async fn lease_file_version(
    middleware: &dyn MongoDao,
    fuuid: &str,
    borrower: &str,
    expiry: &DateTime<Utc>
) -> Result<bool, CommonError> {
    let filtre = doc!{
        "fuuid": fuuid,
        "borrower": borrower,
        "lease_date": {"$lt": expiry},
    };
    let ops = doc!{
        "$currentDate": {"lease_date": true}
    };

    let collection = middleware.get_collection(NOM_COLLECTION_JOBS_VERSIONS_LEASES)?;
    match collection.update_one(filtre, ops).await {
        Ok(result) => Ok(result.modified_count == 1 || result.upserted_id.is_some()),
        Err(_) => Ok(false)
    }
}

#[derive(Serialize)]
pub struct LeaseResponse {
    tuuid: String,
    user_id: String,
    metadata: DataChiffre,
    mimetype: Option<String>,
    cuuids: Option<Vec<String>>,
    version: Option<NodeFichierVersionOwned>,  // Option for folders, no file content
    media: Option<MediaOwnedRow>,
    comments: Option<Vec<FileCommentResponse>>,
}

#[derive(Serialize)]
pub struct LeasesResponse {
    ok: bool,
    leases: Vec<LeaseResponse>,
    secret_keys: Vec<ResponseRequestDechiffrageV2Cle>,
}

/// Lease a batch of files for indexing (AI indexing)
pub async fn get_batch_of_fichiersrep_leases<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    request: RequestLeaseForBatchOperation,
    borrower: &str,
    include_comments: bool
) -> Result<Option<LeasesResponse>, CommonError> where M: MongoDaoTyped
{
    let expiry = Utc::now() - Duration::minutes(5);
    let batch_size = request.batch_size.unwrap_or(5);
    // Iterate through files that have a flag_rag == false or undefined.
    let filtre = doc!{
        FIELD_DELETED: false,
        "$or": [
            {borrower: {"$exists": false}},
            {borrower: false},
        ]
    };

    Ok(lease_batch_fichiersrep(mongo, outbound, &expiry, borrower, filtre, batch_size, false, include_comments).await?)
}


/// Lease a batch of files based on a FichiersRep filtre.
pub async fn lease_batch_fichiersrep<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    expiry: &DateTime<Utc>,
    borrower: &str,
    filtre: Document,
    batch_size: usize,
    include_media: bool,
    include_comments: bool
) -> Result<Option<LeasesResponse>, CommonError> where M: MongoDaoTyped {
    let collection = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
    info!("lease_batch_fichiersrep Getting batch for {}, batch size: {}", borrower, batch_size);

    // Built aggregation pipeline on fichiers_rep joining on versions for files
    let mut pipeline = vec![doc! {"$match": filtre}]; // Match on fichiers_rep

    // Move all content under fichierrep doc for easier mapping to CompleteFileRow
    pipeline.push(doc!{"$replaceRoot": {"newRoot": {"fichierrep": "$$ROOT"}}});

    // Join version information
    pipeline.push(doc!{"$lookup": {
            "from": NOM_COLLECTION_VERSIONS,
            "localField": "fichierrep.fuuids_versions.0",
            "foreignField": "fuuid",
            "as": "versions",
        }}
    );
    pipeline.push(doc!{"$addFields": {
        "tuuid": "$fichierrep.tuuid",   // Used for troubleshooting deserialization errors
        "current_version": {"$arrayElemAt": ["$versions", 0]},
    }});
    pipeline.push(doc!{"$unset": "versions"});

    if include_media {
        // Filter out files that have not been processed yet. Note: this will filter out collections/directories.
        pipeline.push(doc!{"$match": {format!("current_version.{}", CHAMP_FLAG_MEDIA_TRAITE): true}});

        // Join media information
        pipeline.push(doc! {"$lookup": {
                "from": NOM_COLLECTION_MEDIA,
                "localField": "current_version.fuuid",
                "foreignField": "fuuid",
                "as": "medias",
            }}
        );
        pipeline.push(doc! {"$addFields": {
            "media": {"$arrayElemAt": ["$medias", 0]},
        }});
        pipeline.push(doc! {"$unset": "medias"})
    }

    if include_comments {
        // Join media information
        pipeline.push(doc! {"$lookup": {
                "from": NOM_COLLECTION_FILE_COMMENTS,
                "localField": "fichierrep.tuuid",
                "foreignField": "tuuid",
                "as": "comments",
            }}
        );
    }

    let mut leases = Vec::with_capacity(batch_size);

    debug!("lease_batch_fichiersrep Pipeline: {:?}", pipeline);

    let mut cursor = collection.aggregate(pipeline).await?;
    while cursor.advance().await? {
        let row = cursor.deserialize_current()?;
        let tuuid = row.get_str("tuuid").unwrap_or("UNKNOWN").to_string();
        let row: CompleteFileRow = match bson::deserialize_from_document(row) {
            Ok(inner) => inner,
            Err(e) => {
                warn!("get_complete_files Deserialization error on tuuid {}: {:?}", tuuid, e);
                continue    // Skip this entry
            }
        };

        let file = row.fichierrep;
        let version = row.current_version;

        // Attempt a lease on the file
        if lease_file(mongo, file.user_id.as_str(), file.tuuid.as_str(), borrower, expiry).await? {
            // Add the file to leases
            let cuuids = match file.path_cuuids {
                None => None,
                Some(cuuids) => {
                    Some(cuuids.into_iter().map(|c| c.to_string()).collect())
                }
            };
            let comments = match row.comments {
                None => None,
                Some(comments) => {
                    Some(comments.into_iter().map(|c| c.into()).collect())
                }
            };

            leases.push(LeaseResponse {
                tuuid: file.tuuid.to_string(),
                user_id: file.user_id.to_string(),
                metadata: file.metadata.into(),
                cuuids,
                mimetype: match file.mimetype { Some(inner) => Some(inner.to_string()), None => None},
                version,
                media: row.media,
                comments,
            });
        }

        if leases.len() >= batch_size {
            break
        }
    }

    info!("lease_batch_fichiersrep Batch for {} ready: {} leases", borrower, leases.len());

    if leases.len() > 0 {
        info!("lease_batch_fichiersrep Fetching decryption keys for {} leases for borrower {}", leases.len(), borrower);
        // Get all decrypted keys for the files
        let mut cle_ids = HashSet::with_capacity(leases.len());
        for lease in &leases {
            if let Some(cle_id) = lease.metadata.cle_id.as_ref() {
                cle_ids.insert(cle_id.as_str());
            } else if let Some(ref_hachage_bytes) = lease.metadata.ref_hachage_bytes.as_ref() {
                cle_ids.insert(ref_hachage_bytes.as_str());  // Legacy
            }
            if let Some(version) = lease.version.as_ref() {
                if let Some(cle_id) = version.cle_id.as_ref() {
                    cle_ids.insert(cle_id.as_str());
                } else {
                    cle_ids.insert(version.fuuid.as_str());  // Legacy
                }
            }
        }
        let secret_keys = get_decrypted_keys_by_id(outbound, &cle_ids.into_iter().collect()).await?;
        Ok(Some(LeasesResponse { ok: true, leases, secret_keys }))
    } else {
        Ok(None)
    }
}

/// Leases a file. Returns true when successful.
async fn lease_file(
    mongo: &dyn MongoDao,
    user_id: &str,
    tuuid: &str,
    borrower: &str,
    expiry: &DateTime<Utc>
) -> Result<bool, CommonError> {
    let filtre = doc!{
        "tuuid": tuuid,
        "user_id": user_id,
        "borrower": borrower,
        "lease_date": {"$lt": expiry},
    };
    let ops = doc!{
        "$currentDate": {"lease_date": true}
    };

    let collection = mongo.get_collection(NOM_COLLECTION_JOBS_LEASES)?;
    match collection
        .update_one(filtre, ops)
        .upsert(true)
        .await
    {
        // Returns true when a lease record is added or updated - this means the lease is successful
        Ok(result) => Ok(result.modified_count == 1 || result.upserted_id.is_some()),
        Err(_) => Ok(false),
    }
}

pub async fn set_flag_image_traitee(
    mongo: &dyn MongoDao,
    fuuid: &str
) -> Result<(), CommonError> {
    // Set flag versionFichiers
    let filtre = doc! { FIELD_FUUID: fuuid };
    let collection = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let ops = doc! {
        "$set": { CHAMP_FLAG_MEDIA_TRAITE: true },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };
    collection.update_many(filtre, ops).await?;
    Ok(())
}

pub async fn remove_lease(
    mongo: &dyn MongoDao,
    fuuid: &str,
    borrower: &str,
) -> Result<(), CommonError> {
    let lease_collection = mongo.get_collection(NOM_COLLECTION_JOBS_VERSIONS_LEASES)?;
    let lease_filtre = doc!{ CHAMP_FUUID: fuuid, "borrower": borrower };
    lease_collection.delete_one(lease_filtre).await?;
    Ok(())
}

pub async fn set_flag_index_traite(mongo: &dyn MongoDao, tuuid: &str) -> Result<(), CommonError> {
    // Set flag version reps (si applicable)
    let filtre = doc! { FIELD_TUUID: tuuid };
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let ops = doc! {
        "$set": { CHAMP_FLAG_INDEX: true },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };
    collection.update_one(filtre, ops).await?;
    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
pub struct RequestReturnFichiersrepLease {
    pub tuuid: String,
    pub user_id: String,
}

pub async fn return_fichiersrep_lease(
    mongo: &dyn MongoDao,
    request: RequestReturnFichiersrepLease,
    borrower: &str,
) -> Result<(), CommonError> {
    // Update flag in fichiersRep
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    let filtre = doc!{ "tuuid": &request.tuuid, "user_id": &request.user_id };
    let ops = doc!{
        "$set": { borrower: true },
        "$currentDate": { FIELD_DATE_MODIFIED: true }
    };
    collection.update_one(filtre, ops).await?;

    // Remove lease
    let collection_leases = mongo.get_collection(NOM_COLLECTION_JOBS_LEASES)?;
    let filtre = doc!{ "tuuid": request.tuuid, "user_id": request.user_id, "borrower": borrower };
    collection_leases.delete_one(filtre).await?;

    Ok(())
}

#[derive(Serialize)]
struct EventTriggerIndexing {}

pub async fn trigger_file_indexing(outbound: &MessageOutboundFacade) -> Result<(), CommonError> {
    let routing = RoutageMessageAction::builder(
        DOMAIN_NAME, EVENT_FILES_TO_INDEX, vec![Securite::L3Protege]).build();
    outbound.emit_event(routing, EventTriggerIndexing{}).await?;
    Ok(())
}

pub async fn set_flag_video_traite(
    mongo: &dyn MongoDao,
    tuuid: Option<&str>,
    fuuid: &str,
    job_id: Option<&str>
) -> Result<(), CommonError> {
    let mut filtre_video = doc! {"fuuid": fuuid};
    if let Some(tuuid) = tuuid.as_ref() {
        filtre_video.insert("tuuids", tuuid.to_string());
    }

    // Set flag versionFichiers
    let collection = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let ops = doc! {
        "$set": {
            CHAMP_FLAG_VIDEO_TRAITE: true,
            CHAMP_FLAG_MEDIA_TRAITE: true,  // legacy pour eviter de laisser un flag actif pre release-2024.9
        },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };
    collection.update_many(filtre_video.clone(), ops).await?;

    // Supprimer job image
    let filtre_job = match job_id {
        Some(inner) => doc!{"job_id": inner},
        None => filtre_video
    };
    let collection = mongo.get_collection(NOM_COLLECTION_VIDEO_JOBS)?;
    collection.delete_many(filtre_job).await?;

    Ok(())
}

pub async fn reset_indexation(mongo: &dyn MongoDao, outbound: &MessageOutboundFacade) -> Result<(), CommonError> {
    debug!("reset_flag_indexe Reset flags pour tous les fichiers");

    let filter_file_reps = doc! {};
    let ops_file_reps = doc! {
        "$set": { CHAMP_FLAG_INDEX: false, "flag_rag": false },
        "$unset": { CHAMP_FLAG_INDEX_ERREUR: true },
        "$currentDate": { FIELD_DATE_MODIFIED: true },
    };

    // Reset tables rep et versions
    let collection = mongo.get_collection(NOM_COLLECTION_FICHIERS_REP)?;
    collection.update_many(filter_file_reps.clone(), ops_file_reps.clone()).await?;

    // Note: some indexing jobs (leases) may already exist, this is ok since we want to reindex everything.

    // Reset le serveur d'indexation
    let routage = RoutageMessageAction::builder(
        DOMAIN_RELAI_SOLR,
        COMMAND_REINDEX_FILES,
        vec![Securite::L3Protege]
    )
        .timeout_blocking(12_000)
        .build();

    match outbound.send_command(routage, ErrorMessage::ok()).await {
        Ok(Some(inner)) => match inner.is_err()? {
            (true, err) => error!("Error requesting reindexation from solr server: {:?}", err),
            _ => debug!("Reindexation command acknowledged")
        },
        Ok(None) => warn!("reset_indexation: No response received"),
        Err(e) => warn!("reset_indexation Likely a timeout, reporting OK: {:?}", e),
    };

    Ok(())
}

/// Regular maintenance of media jobs
pub async fn maintain_file_jobs<M>(
    transaction: &dyn TransactionService,
    mongo: &M,
    outbound: &MessageOutboundFacade,
    trigger_date: Option<&DateTime<Utc>>
) -> Result<(), CommonError> where M: MongoDaoTyped {

    let (
        run_impossible_jobs,
        run_missing_jobs,
        run_restart_jobs,
        run_maintain_leases,
        run_fetch_filehosts
    ) = match trigger_date {
        Some(date) => {
            let hours = date.hour();
            let minutes = date.minute();

            let run_impossible_jobs = hours % 3 == 2 && minutes == 27;
            let run_missing_jobs = minutes % 30 == 13;
            let run_restart_jobs = minutes % 4 == 2;
            let run_maintain_leases = hours % 3 == 2 && minutes == 9;
            let run_fetch_filehosts = minutes == 2;

            (run_impossible_jobs, run_missing_jobs, run_restart_jobs, run_maintain_leases, run_fetch_filehosts)
        },
        None => (true, true, true, true, true)
    };

    if run_impossible_jobs {
        debug!("maintain_file_jobs run_impossible_jobs");
        if let Err(e) = remove_impossible_jobs(transaction, mongo, NOM_COLLECTION_VIDEO_JOBS, 1_000, "processVideo").await {
            error!("maintain_file_jobs Error processing remove_impossible_jobs: {:?}", e);
        }
    }

    if run_missing_jobs {
        debug!("maintain_file_jobs create_missing_jobs_media");
        if let Err(e) = create_missing_jobs_media(mongo, outbound, NOM_COLLECTION_VIDEO_JOBS, CHAMP_FLAG_VIDEO_TRAITE).await {
            error!("maintain_file_jobs Error processing create_missing_jobs_media: {:?}", e);
        }
    }

    if run_restart_jobs {
        debug!("maintain_file_jobs run_restart_jobs (fetch_filehosts?: {})", run_fetch_filehosts);
        if let Err(e) = reactiver_jobs(
            mongo,
            outbound,
            NOM_COLLECTION_VIDEO_JOBS,
            600,
            100,
            "media",
            "processVideo",
            run_fetch_filehosts
        ).await {
            error!("entretien_jobs_expirees Error: {:?}", e);
        }
    }

    if run_maintain_leases {
        debug!("maintain_file_jobs run_maintain_leases");
        if let Err(e) = run_cleanup_leases(mongo).await {
            error!("maintain_file_jobs run_cleanup_leases Error: {:?}", e);
        }
    }

    Ok(())
}

/// Deactivates for good jobs that have never been picked up or that have too many retries.
pub async fn remove_impossible_jobs<M>(
    transaction: &dyn TransactionService,
    mongo: &M,
    nom_collection: &str,
    limit: i64,
    action: &str,
) -> Result<(), CommonError> where M: MongoDaoTyped {

    let expiration = Utc::now() - Duration::hours(8);
    let collection = mongo.get_collection_typed::<BackgroundJob>(nom_collection)?;
    let filtre = doc!{
        "$or": [
            {"retry": {"$gt": CONST_MAX_RETRY}},  // Any job with retry > CONST_MAX_RETRY
            { FIELD_DATE_MODIFIED: {"$lte": expiration} },
        ]
    };
    // info!("remove_impossible_jobs Collection {} action {}, Filtre: {:?}", nom_collection, action, filtre);

    let mut curseur = collection
        .find(filtre)
        .limit(limit)
        .await?;
    while let Some(row) = curseur.next().await {
        let row = row?;

        // Cancel the job, emit a transaction to clear for good
        warn!("remove_impossible_jobs Too many processing failures of type {} on fuuid {:?}, remove job", action, row.fuuid);
        match action {
            "processImage" => {
                match row.fuuid.as_ref() {
                    Some(fuuid) => {
                        // Create transaction to prevent job from running on restore.
                        let transaction_value = TransactionSupprimerJobImageV2 {
                            // tuuid: row.tuuid,
                            fuuid: fuuid.to_owned(),
                            // err: None,
                        };
                        transaction.process_value(
                            DOMAIN_NAME,
                            TRANSACTION_IMAGE_SUPPRIMER_JOB_V2,
                            serde_json::to_value(&transaction_value)?,
                            None
                        ).await?;
                    },
                    None => {
                        warn!("remove_impossible_jobs Job image tuuid:{} with no fuuid, ignore", row.tuuid);
                    }
                }
            },
            "processVideo" => {
                match row.fuuid.as_ref() {
                    Some(fuuid) => {
                        // Create transaction to prevent job from running on restore.
                        let transaction_value = TransactionSupprimerJobVideoV2 {
                            tuuid: row.tuuid,
                            fuuid: fuuid.to_owned(),
                            job_id: row.job_id.clone(),
                        };
                        info!("remove_impossible_jobs Remove video job {:?}", transaction_value);
                        transaction.process_value(
                            DOMAIN_NAME,
                            TRANSACTION_VIDEO_DELETE_JOB_V2,
                            serde_json::to_value(&transaction_value)?,
                            None
                        ).await?;
                    },
                    None => {
                        warn!("remove_impossible_jobs Job video tuuid:{} with no fuuid, ignore", row.tuuid);
                    }
                }
            },
            "processIndex" => {
                // Toggle index flag in reps/versions collections. There are no transactions for indexing.
                set_flag_index_traite(mongo, row.tuuid.as_str()).await?;
            },
            _ => warn!("remove_impossible_jobs Unknown expired job type: {}", action)
        }

        // Ensure job removal
        let filtre = doc!{ "job_id": &row.job_id };
        collection.delete_one(filtre).await?;
    }

    Ok(())
}

#[derive(Deserialize, Debug)]
struct MissingMediaJobMapping {
    // fuuid: String,
    version: NodeFichierVersionOwned,
    fichierreps: Vec<NodeFichierRepOwned>,
    // jobs: Vec<BackgroundJob>,
}

async fn create_missing_jobs_media<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    jobs_collection_name: &str,
    flag_job: &str
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let collection_version = mongo.get_collection(NOM_COLLECTION_VERSIONS)?;
    let collection_jobs = mongo.get_collection_typed::<BackgroundJob>(jobs_collection_name)?;

    // Remove all file indexing jobs with empty filehosts ([]). They will get recreated.
    let filtre_empty = doc! { "filehost_ids.0": {"$exists": false} };
    collection_jobs.delete_many(filtre_empty).await?;

    let pipeline = vec![
        // Find file versions that are not processed and not deleted (at least 1 tuuid).
        doc! { "$match": {flag_job: false, format!("{}.0", CHAMP_TUUIDS): {"$exists": true}} },
        // Restructure to keep version structure intact for mapping
        doc! { "$replaceRoot": {"newRoot": {"_id": "$fuuid", "version": "$$ROOT"}}},
        doc! { "$addFields": {"fuuid": "$version.fuuid"} },  // Copy fuuid back to root for easier troubleshooting
        doc! { "$lookup": {
            "from": jobs_collection_name,
            "localField": "version.fuuid",
            "foreignField": "fuuid",
            "as": "jobs",
        }},
        // Filter out files that already have a job
        doc! { "$match": {"jobs.0": {"$exists": false} } },
        // Get fichiersrep when present
        doc! { "$lookup": {
            "from": NOM_COLLECTION_FICHIERS_REP,
            "localField": "version.fuuid",
            "foreignField": "fuuids_versions",
            "as": "fichierreps",
        }},
    ];
    debug!("create_missing_jobs_media Pipeline: {:?}", pipeline);

    let mut batch = Vec::with_capacity(50);
    let mut cursor = collection_version.aggregate(pipeline).await?;
    while cursor.advance().await? {
        let row = cursor.deserialize_current()?;
        let fuuid = row.get_str("fuuid")?.to_owned();  // Keep tuuid in case of error
        let row: MissingMediaJobMapping = match bson::deserialize_from_document(row) {
            Ok(inner) => inner,
            Err(e) => {
                warn!("create_missing_jobs_media Mapping error on version entry tuuid:{} - SKIPPING: {:?}", fuuid, e);
                continue
            }
        };
        let fichier_version = row.version;

        let fuuid = fichier_version.fuuid.as_str();

        let visites: Vec<&String> = fichier_version.visites.keys().collect();

        // Ensure the "nouveau" visit is not counted
        let visites = visites
            .into_iter()
            .filter(|v| v.as_str() != "nouveau")  // New just created file, assume not ready
            .map(|v| v.as_str())
            .collect();

        for fichier_rep in row.fichierreps {
            let tuuid = fichier_rep.tuuid.as_str();
            let user_id = fichier_rep.user_id.as_str();

            let mimetype = match fichier_rep.mimetype {
                Some(inner) => inner,
                None => fichier_version.mimetype.clone()
            };

            // Produce the job information.
            // Legacy support for old keys that contained decryption information (nonce, format).
            let mut job = match (
                fichier_version.cle_id.as_ref(),
                fichier_version.format.as_ref(),
                fichier_version.nonce.as_ref())
            {
                (Some(cle_id), Some(format), Some(nonce)) => {
                    let format_str: &str = format.into();
                    let mut job = BackgroundJob::new(
                        tuuid,
                        fuuid,
                        mimetype.as_str(),
                        &visites,
                        cle_id.as_str(),
                        format_str,
                        nonce.as_str(),
                    );
                    job.user_id = Some(user_id.to_string());
                    job
                },
                _ => {
                    debug!("Key information for fuuid:{} missing from version, fetch from key", fuuid);
                    // Old format. The keymaster has the key where cle_id == fuuid.
                    let cle_id = fuuid;

                    // Values for format and header (nonce) are available directly from the key.
                    let mut key_information = outbound.get_keys(DOMAIN_NAME, vec![cle_id.to_string()], None).await?;
                    if key_information.len() == 1 {
                        let key = key_information.pop().expect("pop key_information");
                        match (key.key.format.as_ref(), key.key.nonce.as_ref()) {
                            (Some(format), Some(nonce)) => {
                                let format_str: &str = format.into();
                                let job = BackgroundJob::new(
                                    tuuid,
                                    fuuid,
                                    mimetype.as_str(),
                                    &visites,
                                    cle_id,
                                    format_str,
                                    nonce.to_string().as_str()
                                );
                                job
                            },
                            _ => {
                                warn!("Key information missing from keymaster for fuuid: {}", fuuid);
                                continue
                            }
                        }
                    } else {
                        warn!("Key information missing from keymaster for fuuid: {}", fuuid);
                        continue
                    }
                }
            };

            if flag_job == CHAMP_FLAG_VIDEO_TRAITE {
                // Champs supplementaires pour video
                let params_initial = BackgroundJobParams {
                    defaults: Some(true),
                    thumbnails: Some(true),
                    mimetype: None,
                    codec_video: None,
                    codec_audio: None,
                    resolution_video: None,
                    quality_video: None,
                    bitrate_video: None,
                    bitrate_audio: None,
                    preset: None,
                    audio_stream_idx: None,
                    subtitle_stream_idx: None,
                };
                job.params = Some(params_initial);
            }
            job.user_id = Some(user_id.to_string());

            batch.push(job);

            if batch.len() >= 50 {
                // Save batch to database
                debug!("Saving batch of media jobs");
                collection_jobs.insert_many(&batch).await?;
                batch.clear();
            }
        }

        if batch.len() > 0 {
            // Save batch to database
            debug!("Saving last batch of media jobs");
            collection_jobs.insert_many(&batch).await?;
            batch.clear();
        }
    }

    Ok(())
}

/// Resubmits a batch of pending jobs to queue. Reactivates running jobs that have expired.
pub async fn reactiver_jobs<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    nom_collection: &str,
    timeout: i64,
    limit: i64,
    domain: &str,
    action: &str,
    fetch_filehosts: bool
) -> Result<(), CommonError> where M: MongoDaoTyped {
    let collection = mongo.get_collection_typed::<BackgroundJob>(nom_collection)?;

    // Reset le flag des jobs expirees
    let expiration = Utc::now() - Duration::seconds(timeout);
    let filtre = doc!{"etat": VIDEO_CONVERSION_ETAT_RUNNING, "date_maj": {"$lte": expiration}};
    let ops = doc!{
        "$set": {"etat": VIDEO_CONVERSION_ETAT_PENDING},
        "$inc": {"retry": 1},
    };
    let result = collection.update_many(filtre.clone(), ops).await?;
    info!("reactiver_jobs Collection {}, {} filter: {:?}, result {:?}", nom_collection, action, filtre, result);

    let filtre = doc!{"etat": VIDEO_CONVERSION_ETAT_PENDING, "retry": {"$lte": CONST_MAX_RETRY}};

    // Resubmit jobs - duplicates in the Q will be caught when requesting job decryption key
    let mut fuuids = Vec::new();
    let mut curseur = collection
        .find(filtre)
        .limit(limit)
        .await?;
    while let Some(row) = curseur.next().await {
        let row = row?;
        if let Some(fuuid) = row.fuuid.as_ref() {
            fuuids.push(fuuid.to_owned());
        }

        #[allow(unused)]
        let mut metadata = None;    // Anchor, allows setting reference in JobTrigger
        let trigger: JobTrigger = match action {
            "processIndex" => {
                let collection = mongo.get_collection_typed::<NodeFichierRepOwned>(NOM_COLLECTION_FICHIERS_REP)?;
                let filtre = doc!{ FIELD_TUUID: &row.tuuid };
                match collection.find_one(filtre).await? {
                    Some(fichier) => {
                        let mut trigger = JobTrigger::from(&row);
                        metadata = Some(fichier.metadata);
                        trigger.metadata = metadata.as_ref();
                        trigger.path_cuuids = fichier.path_cuuids;
                        trigger
                    },
                    None => {
                        // Cleanup job pour fichier inconnu
                        let filtre = doc!{ "job_id": &row.job_id };
                        collection.delete_one(filtre).await?;
                        Err(CommonError::String(format!("reactiver_jobs Fichier inconnu tuuid (job maintenant supprimee):{}", row.tuuid)))?
                    }
                }
            },
            _ => (&row).into()
        };

        emettre_processing_trigger(outbound, trigger, domain, action).await;
    }

    debug!("reactiver_jobs Job collection {}, fuuids: {}", nom_collection, fuuids.len());

    // Synchronise with core in case some files were received without GrosFichiers being notified.
    if ! fuuids.is_empty() && fetch_filehosts {
        if let Err(e) = sync_jobs_core_filehosts(mongo, outbound, nom_collection, &fuuids).await {
            warn!("reactiver_jobs Error sync jobs with fuuids in core: {:?}", e);
        }
    }

    Ok(())
}


#[derive(Deserialize)]
pub struct FuuidVisitResponseItem {pub fuuid: String, pub visits: HashMap<String, i64>}

#[derive(Serialize)]
struct RequestFilehostsForFuuids<'a> {
    fuuids: &'a Vec<String>
}

#[derive(Deserialize)]
struct ResponseFilehostsForFuuids {
    fuuids: Vec<FuuidVisitResponseItem>
}

async fn sync_jobs_core_filehosts<M>(
    mongo: &M,
    outbound: &MessageOutboundFacade,
    nom_collection: &str,
    fuuids: &Vec<String>
) -> Result<(), CommonError> where M: MongoDaoTyped {
    debug!("Sync filehost_ids for {} fuuids", fuuids.len());
    let routage = RoutageMessageAction::builder(
        DOMAINE_TOPOLOGIE,
        "requestFilehostsForFuuids",
        vec![Securite::L3Protege]
    ).build();
    let request = RequestFilehostsForFuuids { fuuids };
    let response = outbound.send_request(routage, request).await?;

    // Check if this is an error message - note that success is missing the ok=true (old format)
    if let Ok((true, e)) = response.is_err() {
        return Err(CommonError::String(format!("sync_jobs_core_filehosts Error in requestFilehostsForFuuids: {:?}", e)))
    }

    let response: ResponseFilehostsForFuuids = response.message.deserialize()?;

    let collection_jobs = mongo.get_collection(nom_collection)?;
    let mut session = mongo.get_session().await?;
    session.start_transaction().await?;
    match sync_jobs_save_visits(mongo, response, collection_jobs, &mut session).await {
        Ok(_) => {
            session.commit_transaction().await?;
        },
        Err(e) => {
            session.abort_transaction().await?;
            return Err(e);
        }
    }

    Ok(())
}

async fn sync_jobs_save_visits<M>(
    mongo: &M,
    response: ResponseFilehostsForFuuids,
    collection_jobs: Collection<Document>,
    mut session: &mut ClientSession
) -> Result<(), CommonError>
where M: MongoDaoTyped
{
    for fuuid_visits in response.fuuids {
        // Update the jobs table
        let filehost_ids: Vec<&String> = fuuid_visits.visits.keys().collect();
        let filtre = doc! {"fuuid": &fuuid_visits.fuuid};
        let ops = doc! {"$addToSet": {"filehost_ids": {"$each": filehost_ids}}, "$currentDate": {CHAMP_MODIFICATION: true}};
        collection_jobs.update_many(filtre.clone(), ops).await?;

        // Update visits in the versions tables
        sauvegarder_visites(mongo, fuuid_visits.fuuid.as_str(), &fuuid_visits.visits, &mut session).await?;
    }
    Ok(())
}

async fn run_cleanup_leases<M>(
    mongo: &M
) -> Result<(), CommonError> where M: MongoDao {
    // All file leases are expired after 1 hour
    let expired = Utc::now() - Duration::hours(1);
    let filtre = doc!{ "lease_date": { "$lt": expired } };

    let collection = mongo.get_collection(NOM_COLLECTION_JOBS_LEASES)?;
    collection.delete_many(filtre.clone()).await?;

    let collection = mongo.get_collection(NOM_COLLECTION_JOBS_VERSIONS_LEASES)?;
    collection.delete_many(filtre).await?;

    Ok(())
}

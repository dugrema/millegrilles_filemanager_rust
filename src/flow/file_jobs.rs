use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::chrono::{DateTime, Utc};
use millegrilles_common_rust::constantes::Securite;
use millegrilles_common_rust::dechiffrage::DataChiffre;
use millegrilles_common_rust::jwt_simple::prelude::{Deserialize, Serialize};
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::uuid::Uuid;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::mongodb::options::ReturnDocument;
use millegrilles_common_rust::tracing::{debug, error};
use millegrilles_common_rust::bson::serde_helpers::datetime::FromChrono04DateTime;
use millegrilles_common_rust::mongo_serde::{option_chrono_04_datetime, map_chrono_datetime_as_bson_datetime};
use crate::external::mongo::*;

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
            .build();
        if let Err(e) = outbound.send_command(routing, &trigger).await {
            error!("emettre_processing_trigger Erreur emission trigger commande.{}.{} : {:?}", domain, action, e);
        }
    } else {
        for filehost_id in trigger.filehost_ids {
            let routage = RoutageMessageAction::builder(domain, action, vec![Securite::L2Prive])
                .partition(filehost_id)
                .blocking(false)
                .build();
            if let Err(e) = outbound.send_command(routage, &trigger).await {
                error!("emettre_processing_trigger Erreur emission trigger commande.{}.{}.{} : {:?}", domain, filehost_id, action, e);
            }
        }
    }
}
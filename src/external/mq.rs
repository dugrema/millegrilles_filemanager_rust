use millegrilles_common_rust::constantes::*;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::rabbitmq_dao::ConfigRoutingExchange;
use millegrilles_common_rust::v3::impls::messaging_service::MessagingServiceImpl;
use millegrilles_common_rust::v3::models::ConfigQueueV3;
use crate::constants::*;

pub const QUEUE_TTL_DEFAULT: u32 = 30_000;
pub const QUEUE_TICKER: &str = "job_ticker";
pub const QUEUE_EVENTS: &str = "events";
pub const QUEUE_REQUESTS: &str = "requests";
pub const QUEUE_COMMANDS: &str = "commands";
pub const QUEUE_TRANSACTIONS: &str = "transactions";
pub const QUEUE_BACKUP: &str = "backup";

pub const REQUETE_ACTIVITE_RECENTE: &str = "activiteRecente";
pub const REQUETE_FAVORIS: &str = "favoris";
pub const REQUETE_DOCUMENTS_PAR_TUUID: &str = "documentsParTuuid";
pub const REQUETE_DOCUMENTS_PAR_FUUID: &str = "documentsParFuuid";
pub const REQUETE_CONTENU_COLLECTION: &str = "contenuCollection";
pub const REQUETE_GET_CORBEILLE: &str = "getCorbeille";
pub const REQUETE_GET_CLES_FICHIERS: &str = "getClesFichiers";
pub const REQUETE_GET_CLES_STREAM: &str = "getClesStream";
pub const REQUETE_CONFIRMER_ETAT_FUUIDS: &str = "confirmerEtatFuuids";
pub const REQUETE_VERIFIER_ACCES_FUUIDS: &str = "verifierAccesFuuids";
pub const REQUETE_VERIFIER_ACCES_TUUIDS: &str = "verifierAccesTuuids";
pub const REQUETE_SYNC_COLLECTION: &str = "syncCollection";
pub const REQUETE_SYNC_RECENTS: &str = "syncRecents";
pub const REQUETE_SYNC_CORBEILLE: &str = "syncCorbeille";
pub const REQUETE_SYNC_CUUIDS: &str = "syncCuuids";
pub const REQUETE_JOBS_VIDEO: &str = "requeteJobsVideo";
pub const REQUETE_CHARGER_CONTACTS: &str = "chargerContacts";
pub const REQUETE_PARTAGES_USAGER: &str = "getPartagesUsager";
pub const REQUETE_PARTAGES_CONTACT: &str = "getPartagesContact";
pub const REQUETE_INFO_STATISTIQUES: &str = "getInfoStatistiques";
pub const REQUETE_STRUCTURE_REPERTOIRE: &str = "getStructureRepertoire";
pub const REQUETE_JWT_STREAMING: &str = "getJwtStreaming";
pub const REQUETE_SOUS_REPERTOIRES: &str = "getSousRepertoires";
pub const REQUETE_RECHERCHE_INDEX: &str = "rechercheIndex";
pub const REQUETE_SEARCH_INDEX_V2: &str = "searchIndexV2";
pub const REQUETE_INFO_VIDEO: &str = "getInfoVideo";
pub const REQUEST_SYNC_DIRECTORY: &str = "syncDirectory";
pub const REQUEST_FILES_BY_TUUID: &str = "filesByTuuid";

pub const TRANSACTION_NOUVELLE_VERSION: &str = "nouvelleVersion";
pub const TRANSACTION_NOUVELLE_COLLECTION: &str = "nouvelleCollection";
pub const TRANSACTION_AJOUTER_FICHIERS_COLLECTION: &str = "ajouterFichiersCollection";
pub const TRANSACTION_DEPLACER_FICHIERS_COLLECTION: &str = "deplacerFichiersCollection";
pub const TRANSACTION_SUPPRIMER_DOCUMENTS: &str = "supprimerDocuments";
pub const TRANSACTION_RECUPERER_DOCUMENTS: &str = "recupererDocuments";
pub const TRANSACTION_RECUPERER_DOCUMENTS_V2: &str = "recupererDocumentsV2";
pub const TRANSACTION_RECYCLE_ITEMS_V3: &str = "recycleItemsV3";
pub const TRANSACTION_ARCHIVER_DOCUMENTS: &str = "archiverDocuments";
pub const TRANSACTION_ASSOCIER_CONVERSIONS: &str = "associerConversions";
pub const TRANSACTION_ASSOCIER_VIDEO: &str = "associerVideo";
pub const TRANSACTION_DECRIRE_FICHIER: &str = "decrireFichier";
pub const TRANSACTION_DECRIRE_COLLECTION: &str = "decrireCollection";
pub const TRANSACTION_COPIER_FICHIER_TIERS: &str = "copierFichierTiers";
pub const TRANSACTION_SUPPRIMER_VIDEO: &str = "supprimerVideo";
pub const TRANSACTION_IMAGE_SUPPRIMER_JOB: &str = "supprimerJobImage";
pub const TRANSACTION_IMAGE_SUPPRIMER_JOB_V2: &str = "supprimerJobImageV2";
pub const TRANSACTION_VIDEO_SUPPRIMER_JOB: &str = "supprimerJobVideo";
pub const TRANSACTION_VIDEO_SUPPRIMER_JOB_V2: &str = "supprimerJobVideoV2";
pub const TRANSACTION_CONFIRMER_FICHIER_INDEXE: &str = "confirmerFichierIndexe";
pub const TRANSACTION_AJOUTER_CONTACT_LOCAL: &str = "ajouterContactLocal";
pub const TRANSACTION_SUPPRIMER_CONTACTS: &str = "supprimerContacts";
pub const TRANSACTION_PARTAGER_COLLECTIONS: &str = "partagerCollections";
pub const TRANSACTION_SUPPRIMER_PARTAGE_USAGER: &str = "supprimerPartageUsager";
pub const TRANSACTION_SUPPRIMER_ORPHELINS: &str = "supprimerOrphelins";
pub const TRANSACTION_UPDATE_FILE_TEXT_CONTENT: &str = "updateFileTextContent";
pub const TRANSACTION_DELETE_FILE_COMMENT: &str = "deleteFileComment";
// pub const TRANSACTION_DELETE_V2: &str = "deleteV2";
// pub const TRANSACTION_MOVE_V2: &str = "moveV2";
// pub const TRANSACTION_COPY_V2: &str = "copyV2";
// pub const TRANSACTION_PERMANENTLY_DELETE_FILES: &str = "permanentlyDeleteFiles";
pub const TRANSACTION_ADD_WEB_SUBTITLE: &str = "addWebSubtitle";
pub const TRANSACTION_REMOVE_WEB_SUBTITLE: &str = "removeWebSubtitle";
pub const TRANSACTION_FILE_SUMMARY: &str = "fileSummary";

pub const COMMANDE_REINDEXER: &str = "reindexerFichiers";
pub const COMMANDE_COMPLETER_PREVIEWS: &str = "completerPreviews";
pub const COMMANDE_NOUVEAU_FICHIER: &str = "commandeNouveauFichier";
// pub const COMMANDE_ACTIVITE_FUUIDS: &str = "confirmerActiviteFuuids";
// pub const COMMANDE_IMAGE_GET_JOB: &str = "getJobImage";
pub const COMMANDE_JOB_GET_KEY: &str = "jobGetKey";
pub const COMMANDE_VIDEO_TRANSCODER: &str = "transcoderVideo";
pub const COMMANDE_VIDEO_ARRETER_CONVERSION: &str = "arreterVideo";
pub const COMMANDE_VIDEO_DISPONIBLE: &str = "jobConversionVideoDisponible";
// pub const COMMANDE_VIDEO_GET_JOB: &str = "getJobVideo";
pub const COMMANDE_FUUIDS_DOMAINE_LISTE: &str = "fuuidsDomaineListe";
pub const COMMANDE_GET_CLE_JOB_CONVERSION: &str = "getCleJobConversion";
// const COMMANDE_REINDEXER: &str = "";
pub const COMMAND_VISITS: &str = "visits";
pub const COMMAND_CLAIM_ALL_FILES: &str = "claimAllFiles";
// pub const COMMANDE_INDEXATION_GET_JOB: &str = "getJobIndexation";
pub const COMMANDE_RECLAMER_FUUIDS: &str = "reclamerFuuids";  // Really sync with filehosts
pub const COMMAND_LEASE_FOR_IMAGE: &str = "leaseForImage";
pub const COMMAND_LEASE_FOR_INDEX: &str = "leaseForIndex";
pub const COMMAND_LEASE_FOR_RAG: &str = "leaseForRag";
pub const COMMAND_CONFIRM_RAG: &str = "confirmRag";
pub const COMMAND_CONFIRM_INDEX: &str = "confirmIndex";
pub const COMMAND_LEASE_FOR_SUMMARY: &str = "leaseForSummary";
pub const EVENEMENT_MAJ_COLLECTION: &str = "majCollection";
pub const EVENEMENT_MAJ_CONTENU_COLLECTION: &str = "majContenuCollection";
pub const EVENEMENT_FUUID_AJOUTER_FICHIER_COLLECTION: &str = "fuuidAjouterFichierCollection";
pub const EVENEMENT_FUUID_ASSOCIER_CONVERSION: &str = "fuuidAssocierConversion";
pub const EVENEMENT_FUUID_ASSOCIER_VIDEO: &str = "fuuidAssocierVideo";
pub const EVENEMENT_FUUID_COPIER_FICHIER_TIERS: &str = "fuuidCopierFichierTiers";
pub const EVENEMENT_FUUID_DECRIRE_FICHIER: &str = "fuuidDecrireFichier";
pub const EVENEMENT_FUUID_DEPLACER_FICHIER_COLLECTION: &str = "fuuidDeplacerFichierCollection";
pub const EVENEMENT_FUUID_NOUVELLE_VERSION: &str = "fuuidNouvelleVersion";
pub const EVENEMENT_FUUID_CONSIGNE: &str = "fuuidConsigne";
pub const EVENEMENT_FUUID_RECUPERER: &str = "fuuidRecuperer";
pub const EVENEMENT_FUUID_ARCHIVER: &str = "fuuidArchiver";
pub const EVENEMENT_FUUID_RETIRER_COLLECTION: &str = "fuuidRetirerCollection";
pub const EVENEMENT_FUUID_SUPPRIMER_DOCUMENT: &str = "fuuidSupprimerDocument";
pub const EVENEMENT_AJOUTER_FICHIER: &str = "fuuidNouvelleVersion";
pub const EVENEMENT_CONFIRMER_ETAT_FUUIDS: &str = "confirmerEtatFuuids";
pub const EVENEMENT_TRANSCODAGE_PROGRES: &str = "transcodageProgres";
pub const EVENEMENT_FICHIERS_SYNCPRET: &str = "syncPret";
pub const EVENEMENT_FICHIERS_VISITER_FUUIDS: &str = "visiterFuuids";
pub const EVENEMENT_FILEHOST_NEWFUUID: &str = "filehostNewFuuid";
pub const EVENEMENT_FICHIERS_CONSIGNE: &str = "consigne";
pub const EVENEMENT_FICHIERS_SYNC_PRIMAIRE: &str = "syncPrimaire";
pub const EVENEMENT_REINDEXER_CONSIGNATION: &str = "reindexerConsignation";
pub const EVENEMENT_ANNULER_JOB_VIDEO: &str = "annulerJobVideo";
pub const EVENEMENT_RESET_VISITS_CLAIMS: &str = "resetVisitsClaims";
pub const EVENT_FILES_TO_INDEX: &str = "filesToIndex";

pub const EVENT_KEYMASTER_CERTIFICATE: &str = "certMaitreDesCles";

pub fn init_queues(mq: &MessagingServiceImpl) -> Result<(), CommonError> {
    // Configure the queues and add to messaging service (will spawn consumer threads)
    mq.add_named_queue(
        ConfigQueueV3 {
            queue_name: Some(format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_TICKER)),
            routing_keys: vec![
                ConfigRoutingExchange { routing_key: "evenement.ceduleur.ping".to_string(), exchange: Securite::L1Public }
            ],
            ttl: Some(QUEUE_TTL_DEFAULT),
            durable: true,
            autodelete: true,
        })?;

    mq.add_exclusive_queue(
        QUEUE_EVENTS.to_string(),
        ConfigQueueV3 {
            queue_name: None,
            routing_keys: vec![
                ConfigRoutingExchange { routing_key: format!("evenement.{}.{}", DOMAIN_KEYMASTER, EVENT_KEYMASTER_CERTIFICATE), exchange: Securite::L1Public },
                // ConfigRoutingExchange { routing_key: format!("evenement.{}.{}", DOMAINE_TOPOLOGIE, EVENT_TOPOLOGY_CONFIGURATIONFILE), exchange: Securite::L1Public }

                ConfigRoutingExchange {routing_key: format!("evenement.{}.*.{}", DOMAINE_MEDIA_NOM, EVENEMENT_TRANSCODAGE_PROGRES), exchange: Securite::L2Prive},
                ConfigRoutingExchange {routing_key: format!("evenement.{}.{}", DOMAINE_FILECONTROLER_NOM, EVENEMENT_FILEHOST_NEWFUUID), exchange: Securite::L1Public},
                ConfigRoutingExchange {routing_key: format!("evenement.{}.{}", TOPOLOGIE_NOM_DOMAINE, EVENEMENT_RESET_VISITS_CLAIMS), exchange: Securite::L1Public},

                // ConfigRoutingExchange {routing_key: format!("evenement.{}.{}", DOMAINE_FICHIERS_NOM, EVENEMENT_FICHIERS_SYNCPRET), exchange: Securite::L2Prive},
                // ConfigRoutingExchange {routing_key: format!("evenement.{}.{}", DOMAINE_FICHIERS_NOM, EVENEMENT_FICHIERS_VISITER_FUUIDS), exchange: Securite::L2Prive},
                // ConfigRoutingExchange {routing_key: format!("evenement.{}.{}", DOMAINE_FICHIERS_NOM, EVENEMENT_FICHIERS_CONSIGNE), exchange: Securite::L2Prive},
                // ConfigRoutingExchange {routing_key: format!("evenement.{}.{}", DOMAINE_FICHIERS_NOM, EVENEMENT_FICHIERS_SYNC_PRIMAIRE), exchange: Securite::L2Prive},
            ],
            ttl: Some(QUEUE_TTL_DEFAULT),
            durable: false,     // Exclusive queue
            autodelete: true,
        })?;

    mq.add_named_queue(
        ConfigQueueV3 {
            queue_name: Some(format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_REQUESTS)),
            routing_keys: vec![
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_ACTIVITE_RECENTE), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_FAVORIS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_DOCUMENTS_PAR_TUUID), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_GET_CORBEILLE), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_GET_CLES_FICHIERS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_VERIFIER_ACCES_FUUIDS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_VERIFIER_ACCES_TUUIDS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_SYNC_COLLECTION), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_SYNC_CORBEILLE), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_JOBS_VIDEO), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_CHARGER_CONTACTS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_PARTAGES_USAGER), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_PARTAGES_CONTACT), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_INFO_STATISTIQUES), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_STRUCTURE_REPERTOIRE), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_JWT_STREAMING), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_SOUS_REPERTOIRES), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_RECHERCHE_INDEX), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_INFO_VIDEO), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUEST_SYNC_DIRECTORY), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_SEARCH_INDEX_V2), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUEST_FILES_BY_TUUID), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_CONFIRMER_ETAT_FUUIDS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_GET_CLES_FICHIERS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_GET_CLES_STREAM), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_SYNC_CUUIDS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_CONFIRMER_ETAT_FUUIDS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_VERIFIER_ACCES_FUUIDS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_VERIFIER_ACCES_TUUIDS), exchange: Securite::L2Prive },
            ],
            ttl: Some(QUEUE_TTL_DEFAULT),
            durable: true,
            autodelete: false,
        })?;

    mq.add_named_queue(
        ConfigQueueV3 {
            queue_name: Some(format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_COMMANDS)),
            routing_keys: vec![
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMANDE_RECLAMER_FUUIDS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMANDE_NOUVEAU_FICHIER), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMANDE_VIDEO_TRANSCODER), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMANDE_COMPLETER_PREVIEWS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMAND_LEASE_FOR_IMAGE), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMANDE_JOB_GET_KEY), exchange: Securite::L2Prive },

                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMANDE_REINDEXER), exchange: Securite::L3Protege },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMANDE_GET_CLE_JOB_CONVERSION), exchange: Securite::L3Protege },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMAND_VISITS), exchange: Securite::L3Protege },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMAND_CLAIM_ALL_FILES), exchange: Securite::L3Protege },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMAND_LEASE_FOR_INDEX), exchange: Securite::L3Protege },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMAND_CONFIRM_INDEX), exchange: Securite::L3Protege },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMAND_LEASE_FOR_RAG), exchange: Securite::L3Protege },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMAND_CONFIRM_RAG), exchange: Securite::L3Protege },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMAND_LEASE_FOR_SUMMARY), exchange: Securite::L3Protege },
            ],
            ttl: Some(QUEUE_TTL_DEFAULT),
            durable: true,
            autodelete: false,
        })?;

    mq.add_named_queue(
        ConfigQueueV3 {
            queue_name: Some(format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_TRANSACTIONS)),
            routing_keys: vec![
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_NOUVELLE_VERSION), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_NOUVELLE_COLLECTION), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_AJOUTER_FICHIERS_COLLECTION), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_DEPLACER_FICHIERS_COLLECTION), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_SUPPRIMER_DOCUMENTS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_RECUPERER_DOCUMENTS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_RECUPERER_DOCUMENTS_V2), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_RECYCLE_ITEMS_V3), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_ARCHIVER_DOCUMENTS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_DECRIRE_FICHIER), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_DECRIRE_COLLECTION), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_COPIER_FICHIER_TIERS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_SUPPRIMER_VIDEO), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_VIDEO_SUPPRIMER_JOB_V2), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_AJOUTER_CONTACT_LOCAL), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_SUPPRIMER_CONTACTS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_PARTAGER_COLLECTIONS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_SUPPRIMER_PARTAGE_USAGER), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_SUPPRIMER_ORPHELINS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_UPDATE_FILE_TEXT_CONTENT), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_DELETE_FILE_COMMENT), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_ADD_WEB_SUBTITLE), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_REMOVE_WEB_SUBTITLE), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_ASSOCIER_CONVERSIONS), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_ASSOCIER_VIDEO), exchange: Securite::L2Prive },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_IMAGE_SUPPRIMER_JOB_V2), exchange: Securite::L2Prive },

                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_VIDEO_SUPPRIMER_JOB_V2), exchange: Securite::L3Protege },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_CONFIRMER_FICHIER_INDEXE), exchange: Securite::L3Protege },
                ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_FILE_SUMMARY), exchange: Securite::L3Protege },
            ],
            ttl: Some(QUEUE_TTL_DEFAULT),
            durable: true,
            autodelete: false,
        })?;

    mq.add_named_queue(ConfigQueueV3 {
        queue_name: Some(format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_BACKUP)),
        routing_keys: vec![
            ConfigRoutingExchange { routing_key: format!("requete.{}.getNombreTransactions", DOMAIN_NAME), exchange: Securite::L2Prive },
            ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMANDE_DECLENCHER_BACKUP), exchange: Securite::L3Protege },
        ],
        ttl: Some(QUEUE_TTL_DEFAULT),
        durable: true,
        autodelete: true,
    })?;

    Ok(())
}

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
            ],
            ttl: Some(QUEUE_TTL_DEFAULT),
            durable: false,     // Exclusive queue
            autodelete: true,
        })?;

    mq.add_named_queue(
        ConfigQueueV3 {
            queue_name: Some(format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_REQUESTS)),
            routing_keys: vec![
                // ConfigRoutingExchange { routing_key: format!("requete.{}.{}", DOMAIN_NAME, REQUETE_CHARGER_USAGER), exchange: Securite::L1Public },
            ],
            ttl: Some(QUEUE_TTL_DEFAULT),
            durable: true,
            autodelete: false,
        })?;

    mq.add_named_queue(
        ConfigQueueV3 {
            queue_name: Some(format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_COMMANDS)),
            routing_keys: vec![
                // ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, COMMANDE_AUTHENTIFIER_WEBAUTHN), exchange: Securite::L1Public },
            ],
            ttl: Some(QUEUE_TTL_DEFAULT),
            durable: true,
            autodelete: false,
        })?;

    mq.add_named_queue(
        ConfigQueueV3 {
            queue_name: Some(format!("{}/{}", DOMAIN_NAME_NEW, QUEUE_TRANSACTIONS)),
            routing_keys: vec![
                // ConfigRoutingExchange { routing_key: format!("commande.{}.{}", DOMAIN_NAME, TRANSACTION_REGISTER_OTP), exchange: Securite::L1Public },
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

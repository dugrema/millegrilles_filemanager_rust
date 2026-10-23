use millegrilles_common_rust::dechiffrage::DataChiffre;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionNouvelleCollection {
    pub metadata: DataChiffre,
    pub cuuid: Option<String>,      // Insertion dans collection destination
    pub favoris: Option<bool>,
    // New fields used since 2026-10-23, avoids having to look-up the path of cuuids on replay
    pub cuuids: Option<Vec<String>>,
    pub user_id: Option<String>,    // When present, use this value instaad of certificate for user_id
}

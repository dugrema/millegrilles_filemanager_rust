use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::constantes::{Securite, DOMAINE_NOM_MAITREDESCOMPTES};
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::{debug, warn};
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use serde::Serialize;
use crate::constants::REQUEST_GET_USER_LIST;
use crate::external::mongo::*;
use crate::external::mongo_struct::{ContactRow, RowPartagesUsager};
use crate::flow::request_models::{ReponseUsager, ResponseUsersList};

pub async fn get_contacts_user<M>(
    mongo: &M,
    user_id: &str
) -> Result<Vec<ContactRow>, CommonError> where M: MongoDaoTyped {
    let mut contacts = Vec::new();
    let collection = mongo.get_collection_typed::<ContactRow>(
        NOM_COLLECTION_PARTAGE_CONTACT)?;
    let filtre = doc! { CHAMP_CONTACT_USER_ID: user_id };
    let mut curseur = collection.find(filtre).await?;
    while let Some(row) = curseur.next().await {
        contacts.push(row?);
    }
    Ok(contacts)
}

#[derive(Serialize)]
struct RequestGetUsersList<'a> {
    liste_userids: &'a Vec<&'a str>,
}

pub async fn map_user_ids_nom_usager(
    outbound: &MessageOutboundFacade,
    user_ids: &Vec<&str>
) -> Result<Vec<ReponseUsager>, CommonError> {
    debug!("map_user_ids_nom_usager Pour user_ids {:?}", user_ids);

    let routage = RoutageMessageAction::builder(
        DOMAINE_NOM_MAITREDESCOMPTES,
        REQUEST_GET_USER_LIST,
        vec![Securite::L3Protege]
    ).build();

    let request = RequestGetUsersList { liste_userids: user_ids };
    let response = outbound.send_request(routage, request).await?;
    let response: ResponseUsersList = match response.is_err()? {
        (true, err) => return Err(CommonError::String(format!("map_user_ids_nom_usager Error in response: {:?}", err))),
        _ => response.message.deserialize()?
    };

    Ok(response.usagers)
}

/// Returns the list of allowable shared cuuids to query for a given user. Optional other_user_id
/// restricts list of cuuids to directories shared by that other user with the param "user_id".
pub async fn get_shared_cuuids_by_userid<M>(
    mongo: &M,
    user_id: &str,
    other_user_id: Option<&String>,
    shared_contact_id: Option<&String>,
) -> Result<Vec<String>, CommonError> where M: MongoDaoTyped {

    let mut filter = doc! { CHAMP_CONTACT_USER_ID: user_id };
    if let Some(other_user_id) = other_user_id {
        filter.insert(FIELD_USER_ID, other_user_id);
    }
    if let Some(shared_contact_id) = shared_contact_id {
        filter.insert(CHAMP_CONTACT_ID, shared_contact_id);
    }

    let collection_contact = mongo.get_collection_typed::<ContactRow>(NOM_COLLECTION_PARTAGE_CONTACT)?;
    let mut cursor = collection_contact
        .find(filter)
        .limit(1_000)
        .await?;
    let mut contacts = Vec::with_capacity(1_000);
    while let Some(row) = cursor.next().await {
        let row = row?;
        contacts.push(row.contact_id);
    }
    if contacts.len() == 1000 {
        warn!("get_shared_cuuids_by_userid Shared contact limit hit for user_id: {:?}, not all files may be loaded", user_id);
    }

    let mut cuuids = Vec::with_capacity(1_000);
    let filter = doc! { CHAMP_CONTACT_ID: { "$in": contacts } };
    let collection_shares = mongo.get_collection_typed::<RowPartagesUsager>(NOM_COLLECTION_PARTAGE_COLLECTIONS)?;
    let mut cursor = collection_shares
        .find(filter)
        .limit(1_000)
        .await?;
    while let Some(row) = cursor.next().await {
        cuuids.push(row?.tuuid);
    }
    if cuuids.len() == 1000 {
        warn!("get_shared_cuuids_by_userid Shared collection limit hit for user_id: {:?}, not all files may be loaded", user_id);
    }

    Ok(cuuids)
}

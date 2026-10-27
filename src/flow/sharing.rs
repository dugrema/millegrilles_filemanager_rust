use millegrilles_common_rust::bson::doc;
use millegrilles_common_rust::constantes::{Securite, DOMAINE_NOM_MAITREDESCOMPTES};
use millegrilles_common_rust::mongo_dao::MongoDaoTyped;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::generateur_messages::RoutageMessageAction;
use millegrilles_common_rust::tokio_stream::StreamExt;
use millegrilles_common_rust::tracing::debug;
use millegrilles_common_rust::v3::facades::message_outbound::MessageOutboundFacade;
use serde::Serialize;
use crate::constants::REQUEST_GET_USER_LIST;
use crate::external::mongo::*;
use crate::external::mongo_struct::ContactRow;
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

use millegrilles_common_rust::bson::Bson;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TypeNode {
    Collection,
    Repertoire,
    Fichier,
}

impl TypeNode {
    pub fn to_str(&self) -> &'static str {
        match self {
            TypeNode::Collection => "Collection",
            TypeNode::Repertoire => "Repertoire",
            TypeNode::Fichier => "Fichier",
        }
    }
}

impl Into<&str> for TypeNode {
    fn into(self) -> &'static str {
        self.to_str()
    }
}

impl TryFrom<&str> for TypeNode {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let resultat = match value {
            "Collection" => TypeNode::Collection,
            "Repertoire" => TypeNode::Repertoire,
            "Fichier" => TypeNode::Fichier,
            _ => Err(format!("TypeNode {} non supporte", value))?
        };
        Ok(resultat)
    }
}

impl Into<Bson> for TypeNode {
    fn into(self) -> Bson {
        let into_str: &str = self.into();
        Bson::String(into_str.to_owned())
    }
}

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

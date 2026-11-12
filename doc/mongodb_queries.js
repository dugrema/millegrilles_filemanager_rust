// Deleted file filesize by day

db['FileManager/versionsFichiers'].aggregate([
    { "$match": { tuuids: [] } },
    { "$project": { _id: 1, taille: 1, modified: 1 } },
    { "$group": {
        _id: { "$dateTrunc": { date: "$modified", unit: "day" } },
        taille: { "$sum": "$taille" } },
        nombre: { "$sum": 1 },
    },
    { "$sort": { _id: 1 } }
])

// One line
db['FileManager/versionsFichiers'].aggregate([ { "$match": { tuuids: [] } }, { "$project": { _id: 1, taille: 1, modified: 1 } }, { "$group": {_id: { "$dateTrunc": { date: "$modified", unit: "day" } },taille: { "$sum": "$taille" } , nombre: { "$sum": 1 }}},{ "$sort": { _id: 1 } }])


// List older files
db['FileManager/versionsFichiers']
    .find({ tuuids: [], modified: {"$lte": ISODate("2026-11-04T00:00:00.000Z")} })
    .projection({"fuuid": 1});

db['FileManager/versionsFichiers'].find({ tuuids: [], modified: {"$lte": ISODate("2026-11-11T00:00:00.000Z")} }).projection({"_id": 0, "fuuid": 1})

// Unclaim check in CoreTopologie

db['CoreTopologie/filehostingFuuids'].find({"unclaim_check": {"$exists": 1}})


// List files that can be deleted
db['FileManager/versionsFichiers']
    .find({ tuuids: [], modified: {"$lt": ISODate("2026-11-12T00:00:00.000Z")} })
    .projection({ fuuid: 1, tuuids: 1, visites: 1, modified: 1 });

db['FileManager/versionsFichiers']
    .find({ tuuids: [], visites: {} })
    .projection({"fuuid": 1});

db['FileManager/versionsFichiers']
    .updateOne({fuuid: "zSEfXUD81q9aywY2LqrhBEtdd6aY927QGJrj5U4378h32PnFmAAHy8QcYwnJfAgNKzfyK9MmWJn3iNUyUZX7BMBgunntXH"}, {"$unset": {visites: 1}});

// Get the list of filehost ids present in versions fichiers
db['FileManager/versionsFichiers'].aggregate([
    {"$project": { keyValuePairs: {"$objectToArray": "$visites"} } },
    {"$unwind": "$keyValuePairs"},
    {
        "$group": {
            _id: null,
            filehostIds: {"$addToSet": "$keyValuePairs.k"}
        }
    },
    {"$project": { _id: 0, filehostIds: 1 }}
]);

db['FileManager/versionsFichiers']
    .find({
        tuuids: [],
        "$or": [
            {visites: {}},
            {
                "$expr": {
                    "$lt": [
                        {"$max": ["$visites.9653371d7569f791d24896545dd4446e14aedc90ecf1532a60901d2a45dd924f", "$visites.9653371d7569f791d24896545dd4446e14aedc90ecf1532a6090dasda4"]},
                        ISODate("2026-11-13T00:00:00.000Z"),
                    ]
                }
            }
        ]
    })
    .projection({_id: 0, "fuuid": 1});

db['FileManager/versionsFichiers'].updateOne(
    {fuuid: "zSEfXUD81q9aywY2LqrhBEtdd6aY927QGJrj5U4378h32PnFmAAHy8QcYwnJfAgNKzfyK9MmWJn3iNUyUZX7BMBgunntXH"},
    {"$set": {"visites.9653371d7569f791d24896545dd4446e14aedc90ecf1532a60901d2a45dd924f": ISODate("2026-11-09T00:00:00.000Z")} }
);

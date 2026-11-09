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
    .project({"fuuid": 1})

db['FileManager/versionsFichiers'].find({ tuuids: [], modified: {"$lte": ISODate("2026-11-11T00:00:00.000Z")} }).projection({"_id": 0, "fuuid": 1})

// Unclaim check in CoreTopologie

db['CoreTopologie/filehostingFuuids'].find({"unclaim_check": {"$exists": 1}})

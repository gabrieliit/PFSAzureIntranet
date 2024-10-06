from CommonDataServices import mongo_store
ConsumerSources={
    "LoadJobs":{
        "Format":"MongoStoreCollection",
        "ConnectionDetails":{
            "DbName":"PFS_MI",
            "CollectionName":"LoadJobs"
        },
        "Owner":"gabrielthomas.v@gmail.com",
    }
}

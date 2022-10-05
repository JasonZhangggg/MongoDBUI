from pymongo import MongoClient

cluster = "mongodb://table_extraction:vlldc3JRnDG2DvW3@cluster0-shard-00-00.afvrd.mongodb.net:27017,cluster0-shard-00-01.afvrd.mongodb.net:27017,cluster0-shard-00-02.afvrd.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-12bs5c-shard-0&authSource=admin&retryWrites=true&w=majority"
client = MongoClient(cluster)

db = client.data

db_Test = db.data
db_Melt_Pool = db.Melt_Pool
db_Refractory_Alloys = db.Refractory_Alloys
db_Super_Alloys = db.Super_Alloys
db_Mechanical_Properties = db.Mechanical_Properties
db_HEA_Creep = db.ElsevierHEACreepPDFs
db_Super_Alloys_Creep = db.ElsevierSuperAlloyPDFs

collection_list = {
    'All': [db_Test, db_Melt_Pool, db_Refractory_Alloys, db_Super_Alloys, db_Mechanical_Properties, db_HEA_Creep,
            db_Super_Alloys_Creep], 'Test': [db_Test], 'Mechanical Properties': [db_Mechanical_Properties],
    'Melt Pool': [db_Melt_Pool], 'Refractory Alloys': [db_Refractory_Alloys], 'Super Alloys': [db_Super_Alloys],
    'Hea Creep': [db_HEA_Creep], 'Super Alloys Creep': [db_Super_Alloys_Creep]}

for c_db in collection_list['All']:
     c_db.update_many({}, {"$set": {"Flag": False}})


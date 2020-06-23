from IndianMedia.mongointf.pymongoconn import DBConnection
from IndianMedia.constants import MongoConsts

def migrate(collection):
    dbcon = DBConnection()
    localCstr = dbcon.getLocalConnectionString()
    remoteClient = dbcon.getRemoteClient()
    db = remoteClient[MongoConsts.DB]
    db.cloneCollection(localCstr, f'{MongoConsts.DB}.{collection}')

def getMigrationScript(collection):
    dbcon = DBConnection()
    db = MongoConsts.DB
    uri = dbcon.getRemoteConnectionString()
    str = f"""
    mongoexport --db={db} --collection={collection} --out={collection}.json
    mongoimport --collection={collection} --file={collection}.json --uri={uri}
    """
    return str

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--collection" , help="Name of the collection to migrate")

    args = parser.parse_args()

    print(getMigrationScript(args.collection))

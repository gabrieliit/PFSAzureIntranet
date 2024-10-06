#import other modules
from CommonDataServices import mongo_store, data_utils as du
from ProducerServices.Transform import load_datamaps,transform_utils as tu

class DataQuery():
    def __init__(self,db_name,db_type):
        self.db_type=db_type
        if db_type=="MongoStore":
            self.db=mongo_store.mongo_client[db_name]
    
    def find(self,dataset,filter={}, columns={},dtype="native",ret_type='df'):
        if self.db_type=="MongoStore":
            #transform list of records into df
            if ret_type=='docs':
                docs=list(self.db[dataset].find(filter,projection=columns))
                return docs
            else:
                cursor=self.db[dataset].find(filter,projection=columns)
                df=du.mdb_query_postproc(cursor,dtype)
                return df
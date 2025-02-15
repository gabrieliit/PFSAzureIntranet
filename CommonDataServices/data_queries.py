#import other modules
from CommonDataServices import mongo_store, data_utils as du, transform_utils as tu
from ProducerServices.Transform import load_datamaps

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
    
    def aggregate(self,dataset,agg_pipe):
        try:
            cursor=self.db[dataset].aggregate(agg_pipe)
            df=du.mdb_query_postproc(cursor)
        except Exception as e:
            print(e)
        return df
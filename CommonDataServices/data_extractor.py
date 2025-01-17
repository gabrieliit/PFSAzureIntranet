import pandas as pd
from ProducerServices.Extract.source_metadata import ProducerSources
from ConsumerServices.DatasetTools.consumer_sources_metadata import ConsumerSources
import importlib
import requests
from pymongo.errors import WriteError
from CommonDataServices import data_queries as dq, data_utils as du,mongo_store, transform_utils as tu

class DataSource_Old(): # this should be inherited from DataSource
    def __init__(self,source,df):    
        self.source=source
        try:
            self.pp_ops=ProducerSources[source]["PreProcessOps"]
        except KeyError as e:
            self.pp_ops=[]#no pre-process ops for this source    
        n_drop_rows=ProducerSources[source]["DropRowsEnd"]
        df.drop(df.tail(n_drop_rows).index, inplace=True)
        df.columns=ProducerSources[source]["ColTypes"].keys()
        self.df=df
        self.pre_process_data()
        #dd-mm-yyyy type cols are converted to date types. Convert them back to strings
        dtypes=ProducerSources[source]["ColTypes"]
        #explicity cast data types to match target schema. 
        #NOTE - this shoudl be done as  part of transform not extract processing
        for col,dtype in dtypes.items(): df[col]=du.cast_dfcol_to_type(df[col],dtype)

    def pre_process_data(self):
        for op in self.pp_ops:
            op_name=next(iter(op))
            if op_name=="DerivedCols":
                if op[op_name]["Method"]=="SplitCol":
                    src_col=op[op_name]["SourceCols"][0]
                    method=op[op_name]["SplitMethod"]
                    if method=="UseDelimiter":
                        val=op[op_name]["DelimiterVal"]
                        df=self.df[src_col].str.split(val,expand=True)
                        #Name the split columns
                        df.columns=op[op_name]["TargetCols"]
                        for col,dtype in zip(df.columns,op[op_name]["TargetDataTypes"]): df[col]=du.cast_dfcol_to_type(df[col],dtype)
                        self.df=pd.concat([self.df,df], axis=1)
            elif op_name=="TrimCols":
                cols=op[op_name]["Cols"]
                for col in cols:
                    self.df[col]=self.df[col].str.strip()
            elif op_name=="ToUpper":
                cols=op[op_name]["Cols"]
                for col in cols:
                    self.df[col]=self.df[col].str.upper()            

def source_factory(source,dataset_type,**kwargs):
    source_metadata=ConsumerSources[source] if dataset_type=="Consumer" else ProducerSources[source]
    source_type=source_metadata["Format"]
    if source_type=="MongoStoreCollection":
        try:
            source_obj=MDB_Collection(source,dataset_type)
        except KeyError:
            source_obj=f"{source} is an MDB collection. Please include a DbName element in the consumer source metadata under connection details"
    elif source_type=="API":
        try:
            source_obj=API(source,dataset_type)
        except KeyError:
            f"{source} is an API. Please include a URL and API Key in consumer source metadata under connection details"
    return source_obj

class DataSource():
    def __init__(self,source,dataset_type):
        metadata = ConsumerSources[source] if dataset_type=="Consumer" else ProducerSources[source]
        self.dataset_type=dataset_type
        self.source=source
        self.format=metadata["Format"]
        self.owner=metadata["Owner"]
        try:
            self.agg_defs_module= metadata["AggDefsModule"]
        except KeyError:
            self.agg_defs_module=None
    def load_data(self,filters={},columns={},dtype="native",ret_type='df'):
        self.data=self.query_handler.find(self.source,filters,columns,dtype=dtype,ret_type=ret_type)# at the moment we get all the col from the source. In future we may add filter for cols if use case comes up


            
class MDB_Collection(DataSource):
    def __init__(self, source,dataset_type):
        super().__init__(source,dataset_type)
        metadata = ConsumerSources[source] if dataset_type=="Consumer" else ProducerSources[source]
        db_name=metadata["ConnectionDetails"]["DbName"]
        self.collection=metadata["ConnectionDetails"]["CollectionName"]
        self.query_handler=dq.DataQuery(db_name,db_type="MongoStore")
        self.db=mongo_store.mongo_client[db_name]

    def aggregate(self,agg_pipe_name,as_of_date=None,agg_params={}):
        module=importlib.import_module(self.agg_defs_module)
        agg_pipe_builder=getattr(module,agg_pipe_name)(as_of_date,agg_params)#create an agg_pipe_builder object from class defn in the agg defs module
        return self.query_handler.aggregate(self.collection,agg_pipe_builder.agg_pipe_def)

    def find(self,filter={}, columns={},dtype="native",ret_type='df'):
        cursor=self.db[self.source].find(filter,projection=columns)
        results=self.format_results(cursor,ret_type,dtype)
        status="Success" if len(results)>0 else "NotFound"
        results={"Results":results,"Params":filter,"Status":status,"Message":"Query successful"}
        return results
    
    def format_results(self,results,ret_type='df',dtype="native"):
        if ret_type=='docs':
            docs=list(results)
            return docs
        else:
            df=du.mdb_query_postproc(results,dtype)
            return df
    
    def insert(self,doc):
        try:
            self.db[self.source].insert_one(doc)
            #pass back the inserted doc id, status and empty error attribute
            result={"InsertedID":doc["_id"],"Status":True,"WriteErrors":None}
        #handle write error
        except WriteError as e:
            result={"Status":"Errors","InsertedID":None,"WriteErrors":str(e)}
        return result
    
class API(DataSource):
    def __init__(self, source,dataset_type):
        super().__init__(source,dataset_type)
        metadata = ConsumerSources[source] if dataset_type=="Consumer" else ProducerSources[source]
        self.url=metadata["ConnectionDetails"]["URL"]
        self.api_key=metadata["ConnectionDetails"]["APIKey"]
    
    def find(self,filter={}, columns={},dtype="native",ret_type='df'):
        url_obj=self.generate_url(filter,columns)
        if url_obj["Status"]=="Success":
            response = requests.get(url_obj["URL"])
            if response.status_code == 200:
                results={"Results":self.format_results(response.json()),"Params":filter,"Status":"Success","Message":"API call successful","HTTPStatusCode":response.status_code}
            else:
                results ={"Results":None,"Status":"Failed","Message":"API call failed","HTTPStatusCode":response.status_code} #return empty df if API call fails
        else:
            results ={"Results":None,"Status":"Failed","Message":url_obj["Message"],"HTTPStatusCode":None} #return empty df if URL generation fails
        return results
    
    def generate_url(self,filter,columns):
        if self.source=="MetalPricesAPI":
            try:
                cob_str=filter['COBDate'].strftime("%Y-%m-%d")
                for ccy in filter['CCYList']:
                    ccy_list=ccy+','
                #drop the trailing comma
                ccy_list=ccy_list[:-1]      
                url=f"https://api.metalpriceapi.com/v1/{cob_str}?api_key={self.api_key}&base={filter['Base']}&currencies={ccy_list}"
                msg="URL generated successfully"
                status="Success"
            except KeyError:
                url=None
                msg=f"For sourcing Gold prices from MetalPrices API, please provide COBDate, Base and CCY in the filter"
                status="Failed"
        return {"URL":url,"Message":msg,"Status":status}
    
    def format_results(self,results):
        if self.source=="MetalPricesAPI":
            GM_PER_TROY_OUNCE=31.1035
            price_per_gm=results["rates"]["INRXAU"]/GM_PER_TROY_OUNCE
            return price_per_gm
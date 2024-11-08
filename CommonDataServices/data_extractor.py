import pandas as pd
from ProducerServices.Extract.source_metadata import ProducerSources
from ConsumerServices.Extract.consumer_sources_metadata import ConsumerSources
from CommonDataServices import data_queries as dq, data_utils as du

class DataSource_Old(): # this should be inherited from DataSource
    def __init__(self,source,df):    
        self.source=source
        self.pp_ops=[]
        try:
            #add derived cols to pre-process tasks
            op=ProducerSources[source]["DerivedCols"]
            op["OpType"]="DerivedCols"
            self.pp_ops.append(op)
        except KeyError as e:
            pass#no derived cols
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
            if op["OpType"]=="DerivedCols":
                if op["Method"]=="SplitCol":
                    src_col=op["SourceCols"][0]
                    method=op["SplitMethod"]
                    if method=="UseDelimiter":
                        val=op["DelimiterVal"]
                        df=self.df[src_col].str.split(val,expand=True)
                        #Name the split columns
                        df.columns=op["TargetCols"]
                        for col,dtype in zip(df.columns,op["TargetDataTypes"]): df[col]=du.cast_dfcol_to_type(df[col],dtype)
                        self.df=pd.concat([self.df,df], axis=1)

def source_factory(source,dataset_type,**kwargs):
    source_metadata=ConsumerSources[source] if dataset_type=="Consumer" else ProducerSources[source]
    source_type=source_metadata["Format"]
    owner=source_metadata["Owner"]
    if source_type=="MongoStoreCollection":
        try:
            source_obj=MDB_Collection(source,owner,dataset_type,kwargs["db_name"])
        except KeyError:
            source_obj=f"{source} is an MDB collection. Please include a db_name argument in the call to source_factory method "
    return source_obj

class DataSource():
    def __init__(self,source,format,owner,user_type):
        self.user_type=user_type
        self.source=source
        self.format=format
        self.owner=owner

    def load_data(self,filters={},columns={},dtype="native",ret_type='df'):
        self.data=self.query_handler.find(self.source,filters,columns,dtype=dtype,ret_type=ret_type)# at the moment we get all the col from the source. In future we may add filter for cols if use case comes up
            
class MDB_Collection(DataSource):
    def __init__(self, source,owner,user_type,db_name):
        super().__init__(source, "MongoDbCollection", owner,user_type)
        self.db=db_name
        self.query_handler=dq.DataQuery(db_name, db_type="MongoStore")

        

#import python packages
import pandas as pd
#import other modules
from ConsumerServices.DatasetTools import dataset_rules as dr
from CommonDataServices import data_extractor as de

class DatasetBuilder():
    def __init__(self,target_dataset) -> None:
        self.target=target_dataset
        self.rules=dr.DatasetBuildRules[target_dataset]
        self.data={}

    def create_dataset(self,filters={},dtype='native'):
        for stage,jobs in self.rules["BuildStages"].items():
            self.data[stage]={}
            for job in jobs:
                if job["BuildType"]=="AttribSelect":
                    for source in job["Sources"]:
                        source_name=source["Name"]
                        if source["Type"]=="ProducerDataset":
                            #create source objects and load data
                            source_obj=de.source_factory(source_name,dataset_type="Consumer",db_name="PFS_MI")
                            source_obj.load_data(filters,columns=source["ColFilters"],ret_type='docs')
                            cols=job["AttribList"].keys()
                            docs=source_obj.data
                            #perform post proc jobs for 
                            # 1. flatten the dict and arrays in the source dataset
                            # 2. rename cols to dataset col name from source col names
                            records=[]
                            for doc in docs:
                                record_set=flatten_and_expand(doc)#each input record is expanded if there are dicts/array elements present
                                for record in record_set:#extract and append individual records
                                    records.append(record)
                            raw_df=pd.DataFrame(records)
                            df=pd.DataFrame()
                            for col in cols:
                                try:
                                    df=pd.concat([df,raw_df[job["AttribList"][col]["AttribPath"]]],axis=1)
                                    df=df.rename(columns={job["AttribList"][col]["AttribPath"]:col})
                                except Exception as e:
                                    print(e)
                elif job["BuildType"]=="ConcatRows":
                    df=pd.DataFrame()
                    for source in job["Sources"]:
                        if source["Type"]=="StageOutput":
                            source_stage=source["Stage"]
                            source_job_name=source["Name"]
                            df=pd.concat([df,self.data[source_stage][source_job_name]],axis='index')
                df=df.drop_duplicates()
                self.data[stage][job["BuildJobName"]]=df
        op_stage=self.rules["OutputJob"]["Stage"]
        op_job=self.rules["OutputJob"]["BuildJobName"]
        self.output=self.data[op_stage][op_job]

def flatten_and_expand(d,parent_key='',sep='.'):
    dicts={}#holds flattened dicts
    lists={}#holds flattened lists
    scalars={}#holds scalars
    for k, v in d.items(): 
        new_key = f"{parent_key}{sep}{k}" if parent_key else k  
        if isinstance(v, dict):  
            dict_docs=flatten_and_expand(v,new_key,sep)
            dicts[new_key]=dict_docs
        elif isinstance(v,list):
            list_docs=[]#create a list to hold flattened values in the list
            for item in v:
                if isinstance(item,dict):#array of dicts
                    list_docs+=flatten_and_expand(item,new_key,sep)#append flattened dict records to list
            lists[new_key]=list_docs
        else:#scalar
            scalars[new_key]=v
    #combine dicts, arrays, scalars
    records=[]
    #check for multi row elements
    multi_row_elements=[{"ElementType":"dict","AttribName":key} for key, docs in dicts.items() if len(docs)>1]
    multi_row_elements+=[{"ElementType":"list","AttribName":key} for key, docs in lists.items() if len(docs)>1]
    common_attribs={}
    if len(multi_row_elements)>1:
        raise ValueError(f"Multiple array elements in different branches - {multi_row_elements}. Break into seperate datasets with one array element in each branch each and then join")
    elif len(multi_row_elements)==1:
        #exactly one multi row element. Add that to the list first to create the right number of rows
        mre=multi_row_elements[0]
        mre_source,sre_source=(dicts,lists) if mre["ElementType"]=="dict" else (lists,dicts)
        records=mre_source[mre["AttribName"]]
        mre_source.pop(mre["AttribName"])#remove attrib from mre_Source keys so we can add all other items to the list
        for k,v in mre_source.items():#process arrays of single records, left after popping the multi record items 
            common_attribs.update(v[0]) if v else common_attribs.update({})
        for k,v in sre_source.items():
            common_attribs.update(v[0]) if v else common_attribs.update({})
        for k,v in scalars.items():
            common_attribs[k]=v
    else:
        try:
            for k,v in lists.items():#process arrays of single records, since no multi record attributes are present
                common_attribs.update(v[0]) if v else common_attribs.update({})
            for k,v in dicts.items():
                common_attribs.update(v[0]) if v else common_attribs.update({})
            for k,v in scalars.items():
                common_attribs[k]=v
        except IndexError as e:
            print(e)
    if len(records)>0:#multi row records already created 
        for record in records:
            record=record.update(common_attribs)#add common attribs to each row
    else:
        records=[common_attribs]#no multi row attribs. retyrn a single record
    return records
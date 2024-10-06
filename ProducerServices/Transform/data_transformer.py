import numpy as np
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne,InsertOne
import pandas as pd
from flask import session
from datetime import datetime 
#import other modules
from CommonDataServices import mongo_store, data_utils as du
from ProducerServices.Transform import load_datamaps,transform_utils as tu

#what is the sourc

#Transform the data from LoanDelivery source (see SourceMetadata.py) to MongdDb target schema 
class DataTransformer():
    def __init__(self,source_obj,cob_date) -> None:
        self.df=source_obj.df
        self.source=source_obj.source
        self.cob_date=cob_date
        self.rules=load_datamaps.datamap_selector[self.source]
        self.source_filename=self.source+"_"+cob_date
        self.tgt_metadata={}
        self.warnings=[]
        ordered_targets=np.empty(len(list(self.rules.keys())),dtype=object)
        self.db=mongo_store.mongo_client["PFS_MI"]
        self.write_progress={}
        self.n_batches={}#number of batches per target

        for target in list(self.rules.keys()):
            self.tgt_metadata[target]={}
            self.write_progress[target]={"Status":"Not Started","Completion":0.0}
            self.n_batches[target]=0
            """            
            #get current index count. This will be incremented to get the next object ID for the target
            cursor=self.db["IndexCounts"].find({"Collection":target})
            df = du.mdb_query_postproc(cursor,dtype="native")
            self.tgt_metadata[target]["Index"]=int(df.loc[0,"IndexCount"])
            """
            #adds the target in the order in which it will be processed
            ordered_targets[self.rules[target]["OrderRank"]]=target
            #if target has col to row conversions do some preprocessing
            cardinality=self.rules[target]["Cardinality"]
            n_col_to_rows= 1 if cardinality =="OnetoOne" else self.rules[target]["TargetRowsperSourceRow"]
            self.tgt_metadata[target]["n_col_to_rows"]=n_col_to_rows#store for later use
            #check if target has upsert filters defined
            try:
                upsert_filters=self.rules[target]["UpsertFilters"]
            except KeyError:
                upsert_filters=None
            self.tgt_metadata[target]["UpsertFilters"]=upsert_filters#store for later use 
            rules=self.rules[target]["MappingRules"]
            #add dependency order of zero as default where no dependency order is specified
            for attrib,rule in rules.items():
                try:
                    dep_order=rule["DependencyOrder"]
                except KeyError:
                    rule["DependencyOrder"]=0
            #Convert all rule params to lists of n_col_to_rows items (each item goes into a seperate target record)
            rules = dict(sorted(rules.items(), key=lambda item: item[1]['DependencyOrder']))
            for attrib in rules.keys():
                for item in rules[attrib].keys():
                    if type(rules[attrib][item]) is not list:
                        #convert rule param to a list and fill with the scalar parameter
                        rules[attrib][item]=[rules[attrib][item]]*n_col_to_rows
            #store ordered rules dict
            self.rules[target]["MappingRules"]=rules   
        self.targets=ordered_targets 
    
    def transform_data(self,row_limit=float('inf')):
        #popualte target data  model from source data
        target_records={}
        self.row_limit=row_limit
        for target in list(self.targets):
            #initialise doc for each target as an empty list of records
            target_records[target]= [] 
        for index,row in self.df.iterrows():
            if index>=row_limit:
               break 
            for target in list(self.targets):
                #read the mapping rules for the target
                rules=self.rules[target]["MappingRules"]              
                for i in range(self.tgt_metadata[target]["n_col_to_rows"]):#insert n_col_to_rows docs into target collection for each source row
                    record={}
                    add_record=True
                    for attrib,rule in rules.items():
                        """
                        if row["Notice Type"]=="Auction" and attrib=="NotingType" and rule["AttribName"][i] in ["Notice Type","Weight"]:
                            breakpoint()
                        #process index. NOTE: Indices are not advisable in MongoDB schemas, so all unessential indices have been removed from schema.
                        if rule["Source"][i]=="Index": 
                            #increment and store index
                            new_index=self.tgt_metadata[target]["Index"]+1
                            self.tgt_metadata[target]["Index"]=new_index
                            record[attrib]=new_index
                        """                        
                        #process 1 to 1 mappings
                        if rule["Source"][i]=="Attrib":
                            try:
                                #check if the current attrib uses an alt defs
                                alt_attrib_map=rule["AltAttribMap"][i]
                                row_cond=alt_attrib_map[attrib]["Row_Filter"]
                                attr_cond=alt_attrib_map[attrib]["Attr_Filter"]
                                alt_type=alt_attrib_map[attrib]["AltType"]
                                cast_type=alt_attrib_map[attrib]["CastAltValToType"]
                                alt_val=alt_attrib_map[attrib]["AltVal"]
                                use_alt_def=tu.evaluate_condition(row,record,row_cond,attr_cond)
                                if use_alt_def:
                                    if alt_type=="Constant":
                                        record[attrib]=tu.get_constants(self,alt_val,cast_type)
                                else:
                                    record[attrib]=row[rule["AttribName"][i]]#map value from the main col
                            except:
                                #Attribute doesnt have alt defs, just pull the value from mapped column
                                record[attrib]=row[rule["AttribName"][i]]
                        #process attribut mappings, ie where the column is filled with the same value 
                        elif rule["Source"][i]=="Map":
                            if type(rule["MapName"][i][rule["AttribName"][i]])==dict:
                                #the mapping has a further submap based on attribute value keys
                                sub_map=rule["MapName"][i][rule["AttribName"][i]]
                                try:
                                    record[attrib]=sub_map[row[rule["AttribName"][i]]]
                                except KeyError:
                                    add_record=False #not a valid record as this value doesnt exist in submap, dont send to MongoDb
                            else:
                                record[attrib]=rule["MapName"][i][rule["AttribName"][i]]
                        #process COB Date mappings
                        elif rule["Source"][i]=="COBDate":
                            record[attrib]=pd.to_datetime(self.cob_date)
                    record["Source"]=self.source_filename
                    if add_record:
                        target_records[target].append(record) 
        #store target records as an object attribute after all source rows have been processed
        self.target_records= target_records
        tot_recs=0
        for target in self.targets : tot_recs+=len(target_records[target]) 
        self.tot_recs=tot_recs
        msg="### Summary of write operation\n"
        for target,records in self.target_records.items():
            msg+=f"{len(records)} docs were processed for {target} collection.\n"
        return msg

                
    def load_data(self,batch_size=50):
        #send transformed data to mongo db
        results={}
        write_job={
            "UserName":session["user"]["name"],
            "JobStart":datetime.now(),
            "FileName":self.source_filename,
            "nRows":len(self.df),
            "RowLimit":self.row_limit
        }
        job_id={}#dictionary for storing mongodb id object
        for target in self.targets:
            results[target]={}
            write_job["Target"]=target
            #Perpare a list of write ops for a bulk write operation
            write_ops=[]
            if self.tgt_metadata[target]["UpsertFilters"]:
                #Upsert operations
                for record in self.target_records[target]:
                    filters=self.tgt_metadata[target]["UpsertFilters"]
                    update_cols=[item for item in record.keys() if item not in filters]
                    #create a dict of filter vals for the record
                    filter_dict={}
                    for filter in filters:
                        filter_dict[filter]=record[filter]
                    #create a dict of update vals for the record
                    update_dict={}
                    update_dict_arr={}
                    for col in update_cols:
                        opts=self.db[target].options()
                        if opts["validator"] ["$jsonSchema"]["properties"][col]["bsonType"]==["array"]: 
                            update_dict_arr[col]=record[col]
                        else:
                            update_dict[col]=record[col]
                    op=UpdateOne(filter_dict,{"$set":update_dict,"$push":update_dict_arr},upsert=True)
                    write_ops.append(op)
            else:
                #insert operations
                for record in self.target_records[target]:
                    op=InsertOne(record)
                    write_ops.append(op)

            #create batches of write ops
            n_ops=len(write_ops)
            batches=[]
            for i in range(0,n_ops, batch_size):
                batch=write_ops[i:min(i+batch_size,n_ops)]
                batches.append(batch)
            self.n_batches[target]=len(batches)    
            #insert job record in mongo DB
            self.write_progress[target]["Status"]="In Progress"
            self.write_progress[target]["nBatches"]=len(batches)
            self.write_progress[target]["StartTime"]=datetime.now()
            write_job["JobProgress"]=self.write_progress[target]
            self.db["LoadJobs"].insert_one(write_job)
            #execute batches
            batch_id=0
            batch_details=[]
            #define job key for current job. this will be used to update the job record with progress and results of batches
            job_key={
                "UserName":session["user"]["name"],
                "JobStart":write_job["JobStart"],
                "FileName":self.source_filename,
                "Target":target
            }
            #create a consolidated set of write results across batches            
            error_docs=[]
            dup_error_docs=[]
            n_inserts=n_upserts=n_dups=n_matches=0
            results[target]["ErrorStatus"]="NoErrors"
            #process batches            
            for batch in batches:
                results[target][batch_id]={}
                batch_start=datetime.now()
                try:
                    result=self.db[target].bulk_write(batch,ordered=False)
                    results[target][batch_id]["Status"]="Success" 
                    results[target][batch_id]["WriteOpSummary"]={"n_inserted":result.inserted_count,"n_matched":result.matched_count,"n_upserted":result.upserted_count}
                    results[target][batch_id]["BulkWriteErrorHandler"]=[]
                    results[target][batch_id]["BulkWriteErrors"]=[]#No errors. Create an empty list
                    results[target][batch_id]["DuplicateErrors"]=[]#No errors. Create an empty list
                    n_inserts+=result.inserted_count
                    n_matches+=result.matched_count
                    n_upserts+=result.upserted_count
                except BulkWriteError as bwe:
                    self.errors=bwe.details["writeErrors"]
                    results[target][batch_id]["Status"]="WriteErrors"
                    bwe_summary=BulkWriteErrorHandler(self.source_filename,target,bwe)
                    results[target][batch_id]["BulkWriteErrors"]=bwe_summary.errors_summary_docs
                    results[target][batch_id]["DuplicateErrors"]=bwe_summary.duplicate_errors
                    results[target][batch_id]["n_inserted"]=bwe.details["nInserted"]
                    results[target][batch_id]["n_matched"]=bwe.details["nMatched"]
                    results[target][batch_id]["n_upserted"]=bwe.details["nUpserted"]
                    results[target][batch_id]["n_duplicates"]=len(bwe_summary.duplicate_errors)
                    error_docs+=bwe_summary.errors_summary_docs
                    dup_error_docs+=bwe_summary.duplicate_errors
                    n_inserts+=results[target][batch_id]["n_inserted"]
                    n_upserts+=results[target][batch_id]["n_upserted"]
                    n_dups+=results[target][batch_id]["n_duplicates"]
                    n_matches+=results[target][batch_id]["n_matched"]
                    #if any batch has write errors, update status of target collection as write errors
                    results[target]["ErrorStatus"]="WriteErrors" 
                self.write_progress[target]["Completion"]=(batch_id+1)/len(batches)*100
                batch_end=datetime.now()
                batch_result=results[target][batch_id]["Status"]
                batch_record={
                    "BatchID":batch_id,
                    "BatchStatus":"Complete",
                    "BatchStart":batch_start,
                    "BatchEnd":batch_end,
                    "BatchResult":batch_result,
                }
                batch_details.append(batch_record)
                self.write_progress[target]["BatchDetails"]=batch_details
                #update write progress of the batch, to store updated batch write record and completion % for target collection
                try:
                    self.db["LoadJobs"].update_one(job_key,{"$set":{"JobProgress":self.write_progress[target]}},upsert=False)
                except Exception as e:
                    print(e)
                batch_id+=1
            self.write_progress[target]["Status"]="Complete"
            self.write_progress[target]["EndTime"]=datetime.now()
            msg=f"Inserted {n_inserts}, Upserted {n_upserts}, Matched {n_matches} docs. {n_dups} duplicate docs found"
            status=results[target]["ErrorStatus"]
            write_op_summary=f"\n{target} "+f"{status}"+f" - {msg}"
            results[target]["BulkWriteErrors"]=error_docs
            results[target]["DuplicateErrors"]=dup_error_docs
            self.write_progress[target]["WriteOpSummary"]=write_op_summary
            # Create sub-dictionary
            keys_to_persist={"ErrorStatus","BulkWriteErrors","DuplicateErrors"}
            results_to_persist = {key: results[target][key] for key in keys_to_persist if key in results[target]}
            #update completion of target collection updates
            set_dict={
                "JobProgress":self.write_progress[target],
                "ErrorDetails":results_to_persist
            }
            try:
                self.db["LoadJobs"].update_one(job_key,{"$set":set_dict},upsert=False)
            except Exception as e:
                print(e)
            try:
                id=write_job.pop("_id")#clear object id from previous iteration
                job_id[target]=id
            except KeyError:
                pass
        return job_id
    
class BulkWriteErrorHandler():
    def __init__(self,source, target, bulk_write_errors):
        self.errors=bulk_write_errors
        self.source=source
        self.target=target
        self.errors_summary_docs=[]
        self.duplicate_errors=[]
        for error in bulk_write_errors.details["writeErrors"]:
            error_details={}
            error_details["Target"]=target
            error_details["Index"]=str(error['index'])
            error_details["Msg"]=error['errmsg']
            try:
                error_details["QueryParams"]=str(error['op']['q'])
                error_details["UpdateParams"]=str(error['op']['u'])
            except Exception as e:
                error_details["QueryParams"]="NA"
                error_details["UpdateParams"]="NA"
            if error['code']==121:
                error_details["Details"]=str(error['errInfo']['details']['schemaRulesNotSatisfied'])
                try:
                    error_details["SubType"]='propertiesNotSatisfied - '+ error['errInfo']['details']['schemaRulesNotSatisfied'][0]['propertiesNotSatisfied'][0]['details'][0]['reason']
                except KeyError as e:
                    try:
                        error_details["SubType"]='missingProperties - '+ str(error['errInfo']['details']['schemaRulesNotSatisfied'][0]['missingProperties'])
                    except KeyError:
                        error_details["SubType"]="Unknown Error Sub-Type"
            try:
                if error_details["SubType"]=='propertiesNotSatisfied - found a duplicate item':
                    self.duplicate_errors.append(error_details)
                else:
                    self.errors_summary_docs.append(error_details)
            except KeyError:
                self.errors_summary_docs.append(error_details)
        self.timestamp=datetime.now()
    

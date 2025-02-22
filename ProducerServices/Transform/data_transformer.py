import numpy as np
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne,InsertOne
import pandas as pd
from flask import session
from datetime import datetime
import math
#import other modules
from CommonDataServices import mongo_store, data_utils as du, transform_utils as tu
from ProducerServices.Transform import load_datamaps
from ProducerServices.Aggregations import aggs_map
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
        skip_targets=[]#if some targets are already loaded previously and should not be processed again
        n_targets=len(list(self.rules.keys()))
        ordered_targets=np.empty(n_targets,dtype=object)
        self.db=mongo_store.mongo_client["PFS_MI"]
        self.write_progress={}
        self.n_batches={}#number of batches per target
    
        for target in list(self.rules.keys()):
            if target in skip_targets:
                continue#exit for iteration - do not process targets in skip targets list
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
    
    def transform_data(self,row_limit=float('inf'),batch_size=50):
        #popualte target data  model from source data
        target_records={}
        self.row_limit=row_limit
        self.write_job={
            "UserName":session["user"]["name"],
            "FileName":self.source_filename,
            "nRows":len(self.df),
            "JobStart":datetime.now(),
            "RowLimit":self.row_limit
        }
        self.job_id_list=[]#dictionary for storing mongodb id object        
        for target in list(self.targets):
            if not target: continue # dont process if target is skipped
            #add status record in LoadJobs collection 
            self.write_job["Target"]=target
            self.write_job["JobProgress"]={}
            self.write_job["JobProgress"]["Status"]="GatheringRecords"
            self.db["LoadJobs"].insert_one(self.write_job)
            self.job_id_list.append(self.write_job.pop("_id"))
            #initialise doc for each target as an empty list of records
            target_records[target]= []
        for target in list(self.targets):
            if not target: continue #dont process targets in skip targets list
            
            for index,row in self.df.iterrows():
                if index>=row_limit:
                    break 
                #read the mapping rules for the target
                rules=self.rules[target]["MappingRules"]
                #process any conditional inclusion rules
                try:
                    incl_rules=self.rules[target]["IncludeCriteria"]
                    row_cond_vals=[]
                    add_record=False
                    for cond in incl_rules:
                        if cond["CondType"]=="SourceValBasedInclusion":
                            key_val=row[cond["KeyAttrib"]]
                            param=cond["InclusionRule"]["Param"]
                            cond_op=cond["InclusionRule"]["CondOp"]                                
                        if cond_op=="Eq":cond_val=True if key_val==param else False
                        elif cond_op=="Gt":cond_val=True if key_val>param else False
                        row_cond_vals.append(cond_val)
                        if cond_val==True:#OR each individual condition
                            add_record=True
                except:
                    add_record=True #no conditional inclusion criteria, process all records
                if add_record:#process row if inclusion criteria not violated
                    for i in range(self.tgt_metadata[target]["n_col_to_rows"]):#insert n_col_to_rows docs into target collection for each source row
                        record={}
                        for attrib,rule in rules.items():   
                            #check if there is a condition defined on the attrib
                            try:
                                attr_cond=rule["Condition"][i]
                                incl_attr=None
                                if attr_cond["CondType"]=="KeyBasedInclusion":
                                    key_val=record[attr_cond["KeyAttrib"]]
                                    param=attr_cond["InclusionRule"]["Param"]
                                    cond_op=attr_cond["InclusionRule"]["CondOp"]
                                elif attr_cond["CondType"]=="SourceKeyBasedInclusion":
                                    key_val=row[attr_cond["KeyAttrib"]]
                                    param=attr_cond["InclusionRule"]["Param"]
                                    cond_op=attr_cond["InclusionRule"]["CondOp"]                                
                                elif attr_cond["CondType"]=="SourceValBasedInclusion":
                                    key_val=row[rule["AttribName"][i]]
                                    param=attr_cond["InclusionRule"]["Param"]
                                    cond_op=attr_cond["InclusionRule"]["CondOp"]
                                elif attr_cond["CondType"]=="RowInclCond":
                                    cond_id=attr_cond["CondIdx"]
                                    incl_attr=row_cond_vals[cond_id]
                                if incl_attr == None:
                                    if cond_op=="Eq":incl_attr=True if key_val==param else False
                                    elif cond_op=="Gt":incl_attr=True if key_val>param else False

                            except KeyError: 
                                incl_attr=True
                            if incl_attr:#process attribute for the record if inclusion criteria for attribute is met                  
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
                                elif rule["Source"][i]=="Constant":
                                    record[attrib]=tu.get_constants(self,rule["ConstName"][i])
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
                                
                                #process conditional maps
                                elif rule["Source"][i]=="ConditionalMap":
                                    key_col=rule["KeyAttrib"][i]
                                    try:
                                        key_val=record[key_col]
                                    except KeyError as e:
                                        add_record=False# not a valid record as the conditional key attribute doesnt exist
                                    val_type=rule["MapName"][i][key_val]["ValType"]
                                    val=rule["MapName"][i][key_val]["Val"]
                                    if val_type=="Constant":
                                        record[attrib]=tu.get_constants(self,val)
                                    elif val_type=="SourceAttrib":
                                        record[attrib]=row[val]
                                #process COB Date mappings
                                elif rule["Source"][i]=="COBDate":
                                    record[attrib]=pd.to_datetime(self.cob_date)
                        #record["Source"]=self.source_filename
                        if add_record:
                            target_records[target].append(record)
                        add_record=True#reset flag for next record 
            #Update job status in LoadJobs after creating records for each target
            job_key={
                "UserName":session["user"]["name"],
                "FileName":self.source_filename,
                "JobStart":self.write_job["JobStart"],
                "Target":target
            }
            n_recs=len(target_records[target])
            n_batches=np.floor(n_recs/batch_size)+1
            job_status={"JobProgress":{"Status":"RecordsReady","nBatches":n_batches}}            
            self.db["LoadJobs"].update_one(filter=job_key,update={'$set':job_status})
        #store target records as an object attribute after all source rows have been processed
        self.target_records= target_records
        tot_recs=0
        for target in self.targets : 
            if not target: continue #ignore targets in skip targets list
            tot_recs+=len(target_records[target]) 
        self.tot_recs=tot_recs
        msg="### Summary of write operation\n"
        for target,records in self.target_records.items():
            msg+=f"{len(records)} docs were processed for {target} collection.\n"
        return self.job_id_list

                
    def load_data(self,batch_size=50):
        #send transformed data to mongo db
        results={}
        for target in self.targets:
            if not target: continue #skip target
            results[target]={}
            #Prepare a list of write ops for a bulk write operation
            n_recs=len(self.target_records[target])
            n_batches=np.floor(n_recs/batch_size)+1
            #update job record in mongo DB
            job_key={
                "UserName":session["user"]["name"],
                "FileName":self.source_filename,
                "JobStart":self.write_job["JobStart"],
                "Target":target
            }
            self.write_progress[target]["Status"]="Writing Records"
            self.write_progress[target]["StartTime"]=datetime.now()
            self.write_progress[target]["nBatches"]=n_batches
            job_progress={"JobProgress":self.write_progress[target]}
            self.db["LoadJobs"].update_one(filter=job_key,update={'$set':job_progress})
            #create a consolidated set of write results across batches            
            error_docs=[]
            dup_error_docs=[]
            n_inserts=n_upserts=n_dups=n_matches=0
            results[target]["ErrorStatus"]="NoErrors"
            #initialize batches
            batch_id=0
            batch_details=[]
            #process batches
            for i in range(0,n_recs, batch_size):
                batch=self.target_records[target][i:min(i+batch_size,n_recs)]
                write_ops=[]          
                results[target][batch_id]={}
                batch_start=datetime.now()
                #Create write operations for the batch
                for record in batch:      
                    if self.tgt_metadata[target]["UpsertFilters"]:  
                        #Upsert operation
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
                #Bulk write batch
                try:
                    result=self.db[target].bulk_write(write_ops,ordered=False)
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
                self.write_progress[target]["Completion"]=(batch_id+1)/n_batches*100
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
            check_sums=[]
            checksum_OK=True
            checkcount_OK=True
            #calculate checksums and verify
            #Execute agg pipeline for checksums
            collection_name=target
            for check in self.rules[target]["CheckAggs"]:
                pipeline_name=check["AggPipeline"] 
                pipeline=aggs_map.AggPipelines[collection_name][pipeline_name]
                agg_result=self.db[collection_name].aggregate(pipeline)
                agg_result=du.mdb_query_postproc(agg_result)
                for mapping in check["AggResultsMap"] :
                    for col,item in mapping.items():
                        #get checkagg val from the db
                        id=item["ID"]
                        id["Source"]=self.source_filename#add filename to id field to filter out the checksums for the current file
                        mdb_val=agg_result
                        for id_col,id_val in id.items():#Find the agg result doc corresponding to the ID and File
                            mdb_val=mdb_val[mdb_val[id_col]==id_val]
                        agg_field=item["AggField"]
                        #create and populate checksum doc
                        checksum_doc={}
                        if not math.isinf(self.row_limit): #filter first  row_limit rows of the file
                            df=self.df.head(self.row_limit)
                        else:
                            df=self.df
                        try:
                            row_filters=item["FileRowFilters"]
                        except KeyError:
                            row_filters=None
                        if row_filters:#filter out rows included in the checkcount
                            for key,val in row_filters.items():
                                df=df[df[key]==val]
                        #calc Checksum
                        if agg_field:
                            try:
                                mdb_sum=float(mdb_val[agg_field].iloc[0])
                            except IndexError:#no rows matching the checksum filters
                                mdb_sum=0.0
                            #get checkagg val from file
                            try:
                                file_sum=df[col].astype(float).sum()
                            except ValueError: # col contains non-numerals
                                file_sum=0.0
                        else:
                            mdb_sum="NA"
                            file_sum="NA"
                        #calc checkcount
                        try:
                            mdb_count=int(mdb_val["CheckCount"].iloc[0])
                        except IndexError:
                            mdb_count=0#no rows matching the checksum filters
                        try:
                            keys=item["CompoundPK"]#if count is based on a compound PK
                            file_count=len(df.drop_duplicates(subset=keys))#count unique PK occurences in df
                            checksum_doc["SourceField"]=f"Unique {keys} combos"
                        except KeyError:
                            file_count=len(df)
                            checksum_doc["SourceField"]=col
                        #find the col that is being aggregated in mdb for checksum
                        agg_col=aggs_map.find_checksum_agg_col(pipeline,agg_field)
                        checksum_doc["MDBField"]=f"{collection_name}.{agg_col}"
                        checksum_doc["MDBFilter"]=f"{str(id)}"
                        checksum_doc["FileSum"]=str(file_sum)
                        checksum_doc["MDBSum"]=str(mdb_sum)
                        checksum_doc["FileCount"]=str(file_count)
                        checksum_doc["MDBCount"]=str(mdb_count)
                        check_sums.append(checksum_doc)
                        if type(file_sum) != str and type(mdb_sum) !=str:
                            if abs(file_sum-mdb_sum)>0.1: checksum_OK=False
                        else:#"NA" assigned to either file_Sum or mdb_sum
                            if file_sum!=mdb_sum:checksum_OK=False
                        if file_count!=mdb_count: checkcount_OK=False
            #update completion of target collection updates and checksums for the job
            set_dict={
                "JobProgress":self.write_progress[target],
                "ErrorDetails":results_to_persist,
                "CheckSums":check_sums,
                "CheckSumOK":checksum_OK,
                "CheckCountOK":checkcount_OK
            }
            self.db["LoadJobs"].update_one(job_key,{"$set":set_dict},upsert=False)

    
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
    

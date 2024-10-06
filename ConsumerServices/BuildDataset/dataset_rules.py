DatasetBuildRules={
    "LoadJobs":
    {        
        "OutputJob":{"Stage":1,"BuildJobName":"LoadJobs_1"},
        "BuildStages":
        {
            1:[
                {                
                    "BuildJobName":"LoadJobs_1",
                    "BuildType":"AttribSelect",
                    "Sources":[
                        {
                            "Name":"LoadJobs",
                            "Type":"ProducerDataset",
                            "ColFilters":
                            {
                                "_id":True,
                                "UserName":True,
                                "JobStart":True,
                                "FileName":True,
                                "nRows":True,
                                "RowLimit":True,
                                "Target":True,
                                "JobProgress":True,
                            }
                        }
                    ],
                    "Cardinality":"OneToOne",
                    "AttribList":
                    {
                        "JobID":{
                            "AttribPath":"_id",
                        },
                        "UserName":{
                            "AttribPath":"UserName",
                        },
                        "JobStart":{
                            "AttribPath":"JobStart",
                        },
                        "FileName":{
                            "AttribPath":"FileName",
                        },
                        "nRows":{
                            "AttribPath":"nRows",
                        },
                        "RowLimit":{
                            "AttribPath":"RowLimit",
                        },
                        "Target":{
                            "AttribPath":"Target",
                        },
                        "Status":{
                            "AttribPath":"JobProgress.Status",
                        },
                        "Completion%":{
                            "AttribPath":"JobProgress.Completion",
                        },
                        "nBatches":{
                            "AttribPath":"JobProgress.nBatches",
                        },
                        "StartTime":{
                            "AttribPath":"JobProgress.StartTime",
                        },
                        "EndTime":{
                            "AttribPath":"JobProgress.EndTime",
                        },
                        "Summary":{
                            "AttribPath":"JobProgress.WriteOpSummary",
                        },
                        #"BatchID":{
                        #    "AttribPath":"JobProgress.BatchDetails.BatchID",
                        #},
                        #"BatchStatus":{
                        #   "AttribPath":"JobProgress.BatchDetails.BatchStatus",
                        #},
                        #"BatchStart":{
                        #    "AttribPath":"JobProgress.BatchDetails.BatchStart",
                        #},
                        #"BatchEnd":{
                        #    "AttribPath":"JobProgress.BatchDetails.BatchEnd",
                        #},
                        #"BatchResult":{
                        #   "AttribPath":"JobProgress.BatchDetails.BatchResult",
                        #}                                 
                    }#End of attrib list Loadjobs_1
                }#End of LoadJobs_1 def
            ]#End of Stage 1 build jobs list       
        }#End of Stage 1 build def
    },#End of Load Jobs dataset def
    "WriteErrors":
    {        
        "OutputJob":{"Stage":2,"BuildJobName":"WriteErrors_3"},
        "BuildStages":
        {
            1:[
                {                
                    "BuildJobName":"WriteErrors_1",
                    "BuildType":"AttribSelect",
                    "Sources":[
                        {
                            "Name":"LoadJobs",
                            "Type":"ProducerDataset",
                            "ColFilters":
                            {
                                "_id":True,
                                "FileName":True,
                                "Target":True,
                                "ErrorDetails.DuplicateErrors":True,
                            }
                        }
                    ],
                    "Cardinality":"OnetoMany",#One to many indicates each source record creates multiple rows in dataset 
                    "CardinalityType":"ArrayAttribSource",#the source column contains an array of items
                    "ArrayAttribs":["Collection","SourceRowNo","ErrorType","SourceRowIDKeys","SourceRowValKeys","ErrorDetails","ErrorSubType"],
                    "AttribList":
                    {
                        "Collection":{
                            "AttribPath":"ErrorDetails.DuplicateErrors.Target",
                        },
                        "SourceRowNo":{
                            "AttribPath":"ErrorDetails.DuplicateErrors.Index",
                        },
                        "ErrorType":{
                            "AttribPath":"ErrorDetails.DuplicateErrors.Msg",
                        },
                        "SourceRowIDKeys":{
                            "AttribPath":"ErrorDetails.DuplicateErrors.QueryParams",
                        },
                        "SourceRowValKeys":{
                            "AttribPath":"ErrorDetails.DuplicateErrors.UpdateParams",
                        },
                        "ErrorDetails":{
                            "AttribPath":"ErrorDetails.DuplicateErrors.Details",
                        },
                        "ErrorSubType":{
                            "AttribPath":"ErrorDetails.DuplicateErrors.SubType",
                        },                        

                    }#End of attrib list WriteErrors_1
                },#End of WriteErrors_1 def
                {                
                    "BuildJobName":"WriteErrors_2",
                    "BuildType":"AttribSelect",
                    "Sources":[
                        {
                            "Name":"LoadJobs",
                            "Type":"ProducerDataset",
                            "ColFilters":
                            {
                                "_id":True,
                                "FileName":True,
                                "Target":True,
                                "ErrorDetails.BulkWriteErrors":True
                            }
                        }
                    ],
                    "Cardinality":"OnetoMany",#One to many indicates each source record creates multiple rows in dataset 
                    "CardinalityType":"ArrayAttribSource",#the source column contains an array of items
                    "AttribList":
                    {
                        "Collection":{
                            "AttribPath":"ErrorDetails.BulkWriteErrors.Target",
                        },
                        "SourceRowNo":{
                            "AttribPath":"ErrorDetails.BulkWriteErrors.Index",
                        },
                        "SourceRowNo":{
                            "AttribPath":"ErrorDetails.BulkWriteErrors.Index",
                        },
                        "SourceRowIDKeys":{
                            "AttribPath":"ErrorDetails.BulkWriteErrors.QueryParams",
                        },
                        "SourceRowValKeys":{
                            "AttribPath":"ErrorDetails.BulkWriteErrors.UpdateParams",
                        },
                        "ErrorDetails":{
                            "AttribPath":"ErrorDetails.BulkWriteErrors.Details",
                        },
                        "ErrorType":{
                            "AttribPath":"ErrorDetails.BulkWriteErrors.SubType",
                        },                        
                    }#End of attrib list WriteErrors_2
                },#End of WriteErrors_2 def                
            ],#End of Stage 1 build jobs list for WriteErrors dataset       
            2:[
                {
                    "BuildJobName":"WriteErrors_3",
                    "BuildType":"ConcatenateRows",
                    "Sources":[
                        {
                            "Name":"WriteErrors_1",
                            "Type":"StageOutput",
                            "Stage":1,
                            "ColTag":"DuplicateError"#add this tag to the  rows from this source
                        },
                        {
                            "Name":"WriteErrors_2",
                            "Type":"StageOutput",
                            "Stage":1,
                            "ColTag":"BulkWriteError"#add this tag to the  rows from this source
                        },                        
                    ],
                    "Cardinality":"OnetoOne",#One to many indicates each source record creates multiple rows in dataset 
                    "BuildType":"ConcatRows",#Create a consolidated dataset by concatenating rows
                    "TagColName":"ErrorType"#Add new col to store the ColTags
                }   
            ]
        }#End of Stage 1 build def for WriteErrors datase
    }#End of WriteErrors dataset def
}
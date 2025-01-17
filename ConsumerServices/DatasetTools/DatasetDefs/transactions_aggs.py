from ConsumerServices.DatasetTools.DatasetDefs import aggregations_base as agg
from dateutil.relativedelta import relativedelta as rd

class ReceiptsCount1Q(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "Date": 
                    {
                        "$lte": as_of_date,
                        "$gt" : as_of_date - rd(months=3)
                    }              
                }
            },#Filter out all receipts in last quarter       
            {
                "$group":{
                    "_id": "$RecNo",
                }
            },#group by receipt number
            {
                "$count":"ReceiptCount"
            }#count number of unique receipts
        ]
        return pipe_def

class ReceiptsSize1Q(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "Date": 
                    {
                        "$lte": as_of_date,
                        "$gt" : as_of_date - rd(months=3)
                    }        
                }
            },#Filter out all receipts  in last quarter   
            {
                "$group":
                {
                    "_id": "$TxnType",
                    "TotalReceipts": 
                    {
                        "$sum": "$Amount"
                    },
                }
            }
        ]
        return pipe_def

class DailyReceipts(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "Date": 
                    {
                        "$lte": params["EndDate"],
                        "$gte" : params["StartDate"]
                    }        
                }
            },#Filter out all receipts between start and end date 
            {
                "$group":
                {
                    "_id": "$Date",
                    "TotalReceipts": 
                    {
                        "$sum": "$Amount"
                    },
                }
            },#Sum up daily receipts
            {
                #project id field as Date, and drop _id field
                {
                    "$project": {
                        "Date": "$_id",
                        "_id": 0,
                        "TotalOutflows": 1
                    }
                }
            }            
        ]
        return pipe_def


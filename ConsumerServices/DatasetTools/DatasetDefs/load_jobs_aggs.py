from ConsumerServices.DatasetTools.DatasetDefs import aggregations_base as agg

class LatestCOBDate(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date=None,params={}):
        pipe_def=[
            {
                "$addFields":
                {
                    "COBDateString": {
                        "$substr": [
                            "$FileName",
                            {"$subtract": [{"$strLenCP": "$FileName"},10]},
                            10
                        ]
                    }
                }
            },
            {
                "$addFields":
                {
                    "COBDate": {
                        "$dateFromString": {"dateString": "$COBDateString"}
                    }
                }
            },
            {
                "$group":
                {
                    "_id": "",
                    "MaxCOBDate": 
                    {
                        "$max": "$COBDate"
                    }
                }
            }
        ]
        return pipe_def

class COBDateList(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date=None,params={}):
        pipe_def=[
            {
                "$addFields":
                {
                    "COBDateString": {
                        "$substr": [
                            "$FileName",
                            {"$subtract": [{"$strLenCP": "$FileName"},10]},
                            10
                        ]
                    }
                }
            },
            {
                "$addFields":
                {
                    "COBDate": {
                        "$dateFromString": {"dateString": "$COBDateString"}
                    }
                }
            },
            {
                "$group":
                {
                    "_id": "",
                    "COBDateList": 
                    {
                        "$addToSet": "$COBDate"
                    }
                }
            }
        ]
        return pipe_def

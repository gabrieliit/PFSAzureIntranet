from ProducerServices.Transform import transform_utils
TransformMap={
    "Transactions":
    {
        "OrderRank":0,
        "Cardinality":"OnetoMany",
        "TargetRowsperSourceRow":2,
        "MappingRules":
        {
            "Date":
            {
                "Source":"Attrib",
                "AttribName":"Rec. Dt"
            },
            "RecNo":
            {
                "Source":"Attrib",
                "AttribName":"Rec. No"
            },
            "GLNo":
            {
                "Source":"Attrib",
                "AttribName":"GL. No.",
            },
            "TxnType":
            {
                "Source":"Map",
                "MapName":transform_utils.TxnTypeMap,
                 "AttribName":[
                     "Prici. Rec",
                     "Int. Rec."
                 ]
            },             
            "Amount":
            {
                "Source":"Attrib",
                 "AttribName":[
                     "Prici. Rec",
                     "Int. Rec."
                 ]
            },
            "PrincDue":
            {
                "Source":"Attrib",
                "AttribName":"Int. Calculated On"
            },
            "PendingDays":
            {
                "Source":"Attrib",
                "AttribName":"No. of Days"
            },
        },
        "UpsertFilters":["GLNo","RecNo","TxnType","Date","Amount"],
        "CheckAggs":[
            {            
                "AggPipeline":"CheckSumTxns",
                "AggResultsMap":
                [
                    {"Prici. Rec":{"ID":{"TxnType":"PrincPayment"},"AggField":"ChecksumAmount",}},
                    {"Int. Rec.":{"ID":{"TxnType":"IntPayment"},"AggField":"ChecksumAmount"}},
                ]
            }
        ]
    },
    "Accounts":
    {
        "OrderRank":1,
        "Cardinality":"OnetoOne",
        "MappingRules":
        {
            "GLNo":
            {
                "Source":"Attrib",
                "AttribName":"GL. No.",
            },
            "LoanStatus":
            {
                "Source":"Attrib",
                "AttribName":"ST",
            },
            "LoanStartDate":
            {
                "Source":"Attrib",
                "AttribName":"GL Date"
            }
        },
        "UpsertFilters":["GLNo"],
        "IncludeCriteria":{"FilterType":"AttribVal","AttribName":"ST","IncludedVals":["CL"]},#only add/update records if the source value is in IncludedVals list
        "CheckAggs":[
            {
                "AggPipeline":"CheckCountLoanStatus",
                "AggResultsMap":
                [
                    {"ST":{"ID":{"LoanStatus":"CL"},"AggField":None,"FileRowFilters":{"ST":"CL"},"CompoundPK":["GL. No."]}},#AggField is None so only count will be checked (this is added by default to check agg op hnece not specified)
                ]
            }
        ]
    },
}

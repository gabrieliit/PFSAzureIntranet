from CommonDataServices import transform_utils
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
        "UpsertFilters":["RecNo","TxnType","Date"],
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
            "PrincPaymentAmounts":
            {
                "Source":"Attrib",
                "AttribName":"Prici. Rec",
                "Condition":{
                    "CondType":"RowInclCond",
                    "CondIdx":1,
                },#only add Principle payment if the Princi Rec >0.0 in source file        
            },
            "PrincPaymentDates":
            {
                "Source":"Attrib",
                "AttribName":"Rec. Dt",
                "Condition":{
                    "CondType":"RowInclCond",
                    "CondIdx":1,
                },#only add Principle payment if the Princi Rec >0.0 in source file      
            },
        },
        "UpsertFilters":["GLNo"],
        "IncludeCriteria":
        [
            {
                "CondType":"SourceValBasedInclusion",
                "KeyAttrib":"ST",
                "InclusionRule":{"Param":"CL","CondOp":"Eq"}
            },#only add LoanCLosureDate in the record if the Loan Satus attribute =="CL"
            {
                "CondType":"SourceValBasedInclusion",
                "KeyAttrib":"Prici. Rec",
                "InclusionRule":{"Param":0.0,"CondOp":"Gt"}
            },#only add Principle payment if the Princi Rec >0.0 in source file                      
        ],
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

"""
Temporarily removed 
            "CollWeight":
            {
                "Source":"Attrib",
                "AttribName":"Weight",
                "Condition":{
                    "CondType":"RowInclCond",
                    "CondIdx":0,
                },#only add CollWeight in the record if the Loan Satus attribute =="CL"       
            },
            "LoanStartDate":
            {
                "Source":"Attrib",
                "AttribName":"GL Date"
            },

            "LoanStatus":
            {
                "Source":"Attrib",
                "AttribName":"ST",
                "Condition":{
                    "CondType":"RowInclCond",
                    "CondIdx":0,
                },#only add LoanCLosureDate in the record if the Loan Satus attribute =="CL"             
            },            
            "LoanClosureDate":
            {
                "Source":"Attrib",
                "AttribName":"Rec. Dt",
                "Condition":{
                    "CondType":"RowInclCond",
                    "CondIdx":0,
                },#only add LoanCLosureDate in the record if the Loan Satus attribute =="CL"
                "DependencyOrder":1
            },
"""

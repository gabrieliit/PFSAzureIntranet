from ProducerServices.Transform import transform_utils
TransformMap={
    "AccountNotings":
    {
        "OrderRank":1,
        "Cardinality":"OnetoMany",
        "TargetRowsperSourceRow":2,
        "MappingRules":
        {
            "GLNo":
            {
                "Source":"Attrib",
                "AttribName":"GL. No"
            },            
            "NotingType":
            {
                "Source":"Map",
                "MapName":transform_utils.NotingTypeMap,
                "AttribName":["Interest Due","Princi. Due"]
            },
            "Date":
            {
                "Source":"COBDate"
            },
            "Amount":
            {
                "Source":"Attrib",
                "AttribName":["Interest Due","Princi. Due"]
            }
        },
        "UpsertFilters":["GLNo","NotingType","Date","Amount"],
        "CheckAggs":[
            {
                "AggPipeline":"CheckSumAccountNotings",
                "AggResultsMap":
                [
                    {"Princi. Due":{"ID":{"NotingType":"PrincDue"},"AggField":"CheckSumAmount",}},
                    {"Interest Due":{"ID":{"NotingType":"IntDue"},"AggField":"CheckSumAmount",}},
                ]
            }
        ]
    },
    "Accounts":
    {
        "OrderRank":0,
        "Cardinality":"OnetoOne",
        "MappingRules":
        {
            "GLNo":
            {
                "Source":"Attrib",
                "AttribName":"GL. No"
            },
            "CustName":
            {
                "Source":"Attrib",
                "AttribName":"Name"
            },
            "LoanAmount":
            {
                "Source":"Attrib",
                "AttribName":"Amt. Given"
            },
            "LoanStartDate":
            {
                "Source":"Attrib",
                "AttribName":"Date"
            },
        },
        "UpsertFilters":["GLNo"],
        "CheckAggs":[
            {
                "AggPipeline":"CheckSumAccounts",
                "AggResultsMap":
                [
                    {"Amt. Given":{"ID":{},"AggField":"CheckSumLoanAmount",}}
                ]
            },
        ]
    }
}
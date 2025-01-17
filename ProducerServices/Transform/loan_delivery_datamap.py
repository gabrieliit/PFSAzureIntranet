from CommonDataServices import transform_utils
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
                "AttribName":["Int. Rec.","Princ. Rec."]
            },
            "Date":
            {
                "Source":"COBDate"
            },
            "Amount":
            {
                "Source":"Attrib",
                "AttribName":["Int. Rec.","Princ. Rec."]
            }
        },
        "UpsertFilters":["GLNo","NotingType","Date","Amount"],
        "CheckAggs":[
            {
                "AggPipeline":"CheckSumAccountNotings",
                "AggResultsMap":
                [
                    {"Princ. Rec.":{"ID":{"NotingType":"PrincRec"},"AggField":"CheckSumAmount",}},
                    {"Int. Rec.":{"ID":{"NotingType":"IntRec"},"AggField":"CheckSumAmount",}},
                ]
            }
        ]
    },
    "Accounts":
    {
        "OrderRank":2,
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
            "CustPhone":
            {
                "Source":"Attrib",
                "AttribName":"Phone"
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
            "CollItems":
            {
                "Source":"Attrib",
                "AttribName":"Items",
                "AttribFormat":"CSL"
            },
            "CollWeight":
            {
                "Source":"Attrib",
                "AttribName":"Weight"
            },
        },
        "UpsertFilters":["GLNo"],
        "CheckAggs":[
            {
                "AggPipeline":"CheckSumAccounts",
                "AggResultsMap":
                [
                    {"Weight":{"ID":{},"AggField":"CheckSumWeight",}},
                    {"Amt. Given":{"ID":{},"AggField":"CheckSumLoanAmount",}}
                ]
            },
        ]
    }
}
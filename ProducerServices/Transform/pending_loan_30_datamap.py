from CommonDataServices import transform_utils
TransformMap={
    "AccountNotings":
    {
        "OrderRank":1,
        "Cardinality":"OnetoMany",
        "TargetRowsperSourceRow":11,
        "MappingRules":
        {
            "GLNo":
            {
                "Source":"Attrib",
                "AttribName":"GL No."
            },            
            "NotingType":
            {
                "Source":"Map",
                "MapName":transform_utils.NotingTypeMap,
                "AttribName":[
                    "Principal Balance",
                    "Interest Received",
                    "Interest Outstanding",
                    "Total Outstanding",
                    "Average Val. per Gram",
                    "Market Value",
                    "Difference",
                    "Received Upto",
                    "Pending Days",
                    "Notice Type",
                    "Interest Rate"
                    ]
            },
            "Date":
            {
                "Source":"ConditionalMap",
                "KeyAttrib":"NotingType",
                "MapName":transform_utils.NotingDateMap,
                #This attribute is dependent on at least one lower dependency order attribute.
                # When not specfied dependency order is defaulted to zero, ie no dependencies. 
                "DependencyOrder":1   
            },
            "Amount":
            {
                "Source":"Attrib",
                "AttribName":[                    
                    "Principal Balance",
                    "Interest Received",
                    "Interest Outstanding",
                    "Total Outstanding",
                    "Average Val. per Gram",
                    "Market Value",
                    "Difference",
                    "Received Upto",
                    "Pending Days",
                    "Notice Type",
                    "Interest Rate"
                ]
            }
        },
        "UpsertFilters":["GLNo","NotingType","Date","Amount"],
        "CheckAggs":[
            {
                "AggPipeline":"CheckSumAccountNotings",
                "AggResultsMap":
                [
                    {"Principal Balance":{"ID":{"NotingType":"PrincDue"},"AggField":"CheckSumAmount",}},
                    {"Interest Outstanding":{"ID":{"NotingType":"IntDue"},"AggField":"CheckSumAmount",}},
                    {"Interest Received":{"ID":{"NotingType":"IntRec"},"AggField":"CheckSumAmount",}},
                    {"Notice Type":{"ID":{"NotingType":"Auction"},"AggField":"CheckSumAmount","FileRowFilters":{"Notice Type":"Auction"}}},
                    {"Notice Type":{"ID":{"NotingType":"Notice1"},"AggField":"CheckSumAmount","FileRowFilters":{"Notice Type":"Notice 1"}}},
                    {"Notice Type":{"ID":{"NotingType":"Notice2"},"AggField":"CheckSumAmount","FileRowFilters":{"Notice Type":"Notice 2"}}},
                    {"Interest Rate":{"ID":{"NotingType":"IntRate"},"AggField":"CheckSumAmount",}},
                    {"Average Val. per Gram":{"ID":{"NotingType":"PrincPerGm"},"AggField":"CheckSumAmount",}},
                    {"Market Value":{"ID":{"NotingType":"CollVal"},"AggField":"CheckSumAmount",}},
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
                "AttribName":"GL No."
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
                "AttribName":"Amount Given"
            },
            "LoanStartDate":
            {
                "Source":"Attrib",
                "AttribName":"Date"
            },
            "LoanDueDate":
            {
                "Source":"Attrib",
                "AttribName":"Due Date"
            },
        },
        "UpsertFilters":["GLNo"],
        "CheckAggs":[
            {
                "AggPipeline":"CheckSumAccounts",
                "AggResultsMap":
                [
                    {"Amount Given":{"ID":{},"AggField":"CheckSumLoanAmount",}}
                ]
            },
        ]
    },
}

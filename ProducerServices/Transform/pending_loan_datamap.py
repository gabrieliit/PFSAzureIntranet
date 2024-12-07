from ProducerServices.Transform import transform_utils
TransformMap={
    "Customers":
    {
        "OrderRank":0,
        "Cardinality":"OnetoOne",
        "MappingRules":
        {                
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
            "CustAddress":
            {
                "Source":"Attrib",
                "AttribName":"Address"
            },
            "LoanDates":
            {
                "Source":"Attrib",
                "AttribName":"Date"
            }
        },
        "UpsertFilters":["CustName","CustPhone"],
        "CheckAggs":[               
            {
                "AggPipeline":"CheckSumCustomers",
                "AggResultsMap":
                [
                    {
                        "Name":{"ID":{},"AggField":None,"CompoundPK":["Name","Phone"]}
                    }
                ],#AggField is None so only count will be checked (this is added by default to check agg op hnece not specified)   
            }     
        ]   
    },
    "AccountNotings":
    {
        "OrderRank":2,
        "Cardinality":"OnetoMany",
        "TargetRowsperSourceRow":6,
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
                "AttribName":[
                    "Principal Bal.",
                    "Principal Rec.",
                    "Int. Rec.",
                    "Int. Bal",
                    "Notice Type",
                    "Int. Rate"
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
                    "Principal Bal.",
                    "Principal Rec.",
                    "Int. Rec.",
                    "Int. Bal",
                    "Notice Type",
                    "Int. Rate"
                ]
            }
        },
        "UpsertFilters":["GLNo","NotingType","Date","Amount"],
        "CheckAggs":[
            {
                "AggPipeline":"CheckSumAccountNotings",
                "AggResultsMap":
                [
                    {"Principal Bal.":{"ID":{"NotingType":"PrincDue"},"AggField":"CheckSumAmount",}},
                    {"Principal Rec.":{"ID":{"NotingType":"PrincRec"},"AggField":"CheckSumAmount",}},
                    {"Int. Bal":{"ID":{"NotingType":"IntDue"},"AggField":"CheckSumAmount",}},
                    {"Int. Rec.":{"ID":{"NotingType":"IntRec"},"AggField":"CheckSumAmount",}},
                    {"Notice Type":{"ID":{"NotingType":"Auction"},"AggField":"CheckSumAmount","FileRowFilters":{"Notice Type":"Auction"}}},
                    {"Notice Type":{"ID":{"NotingType":"Notice1"},"AggField":"CheckSumAmount","FileRowFilters":{"Notice Type":"Notice 1"}}},
                    {"Notice Type":{"ID":{"NotingType":"Notice2"},"AggField":"CheckSumAmount","FileRowFilters":{"Notice Type":"Notice 2"}}},
                    {"Int. Rate":{"ID":{"NotingType":"IntRate"},"AggField":"CheckSumAmount",}},
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
            "CollWeight":
            {
                "Source":"Attrib",
                "AttribName":"Weight"
            },
            "LoanDueDate":
            {
                "Source":"Attrib",
                "AttribName":"Due Date"
            },
            "SchemeID":
            {
                "Source":"Attrib",
                "AttribName":"Scheme"
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
    },
}

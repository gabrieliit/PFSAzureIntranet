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
            "LoanDates":
            {
                "Source":"Attrib",
                "AttribName":"Date",
            }
        },
        "UpsertFilters":["CustName","CustPhone"]
    },
    "AccountNotings":
    {
        "OrderRank":2,
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
                "Source":"Attrib",
                "AltAttribMap":transform_utils.NotingDateMap,
                "AttribName":"Notice Date",
                #This attribute is dependent on at least one lower dependency order attribute.
                # When not specfied dependency order is defaulted to zero, ie no dependencies. 
                "DependencyOrder":1            },
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
        "UpsertFilters":["GLNo","NotingType","Date","Amount"]
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
            "CollWeight":
            {
                "Source":"Attrib",
                "AttribName":"Weight"
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
        },
        "UpsertFilters":["GLNo","LoanStartDate","LoanAmount"]
    },
}

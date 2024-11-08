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
                "AttribName":"Date",
            }
        },
        "UpsertFilters":["CustName","CustPhone"]
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
        "UpsertFilters":["GLNo","LoanStartDate","LoanAmount"]
    },
}

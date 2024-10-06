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
        "UpsertFilters":["GLNo","NotingType","Date","Amount"]
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
        "UpsertFilters":["GLNo","LoanAmount","LoanStartDate"]
    }
}
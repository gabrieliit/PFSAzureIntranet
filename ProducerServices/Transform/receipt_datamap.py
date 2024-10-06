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
        "UpsertFilters":["GLNo","RecNo","TxnType","Date","Amount"]
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
                "AttribName":"ST"
            },
        },
        "UpsertFilters":["GLNo"]
    },
}

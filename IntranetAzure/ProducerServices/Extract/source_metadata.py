ProducerSources = {
    "PendingLoan":
    {
        "Merged":False,
        "Description": "Loan details for all loans",
        "FileName":"PendingLoan",
        "SkipRows":4,
        "Format":"xls",
        #These should be valiadted against a data dictionary -TBD
        "ColTypes":{
            "Sl.No":int,
            "Date":"date-%d-%m-%Y",
            "GL. No":str,
            "Name":str,
            "Phone":str,
            "Amt. Given":float,
            "Int. Rate":str,
            "Weight":float,
            "Principal Rec.":str,
            "Principal Bal.":str,
            "Int. Rec.":str,
            "Int. Bal":str,
            "Days":str,
            "Scheme":"intstr",
            "RP":str,
            "Due Date":"date-%d/%m/%Y",
            "Address":str,
            "Notice Type":str,
            "Notice Date":"date-%d/%m/%Y",
            "Notes":str
        },
        "PreProcessOps":[
            {"TrimCols":{"Cols":["Name"]}},
            {"ToUpper":{"Cols":["Name"]}},
        ],       
        "DropRowsEnd":2,#Drop total rows and empty rows at the end
    },
    "LoanStatus":
    {
        "Merged":False,
        "Description":"Loan amounts and balances for pending loans",
        "FileName": "LoanStatus",
        "SkipRows": 4,
        "ColTypes":{"Date":"date-%d-%m-%Y","GL. No":str,"Name":str,"Amt. Given":float,"Interest Due":str,"Princi. Due":str},
        #"DerivedCols":["OverdueSince"],
        "Format":"xls",
        "DropRowsEnd":2,#Drop total rows and empty rows at the end
        "PreProcessOps":[
            {"TrimCols":{"Cols":["Name"]}},
            {"ToUpper":{"Cols":["Name"]}},
        ],  
    },
    "PendingLoan_30":
    {
        "Merged":False,
        "Description": "Loan details for loans with payments due more than 30 days",
        "FileName": "PendingLoan_30",
        "SkipRows": 4,
        "ColTypes": {   
                        "Date":"date-%d-%m-%Y",
                        "GL No.":str,
                        "Name":str,
                        "Amount Given":float,
                        "Interest Rate":str,
                        "Weight":float,
                        "Principal Balance":str,
                        "Interest Received":str,
                        "Interest Outstanding":str,
                        "Total Outstanding":str,
                        "Average Val. per Gram":str,
                        "Market Value":str,
                        "Difference":str,
                        "Received Upto":str,
                        "Pending Days":str,
                        "Scheme":str,
                        "Due Date":"date-%d/%m/%Y",
                        "Phone":str,
                        "Notice Type":str,
                        "Notice Date":"date-%d/%m/%Y",
                        "Notes":str
                    },
        "PreProcessOps":[
            {"TrimCols":{"Cols":["Name"]}},
            {"ToUpper":{"Cols":["Name"]}},
        ],  
        "Format":"xls",
        "DropRowsEnd":2,#Drop total rows and empty rows at the end        
    },
    "Receipt":
    {
        "Merged":False,
        "Description": "Receipt details for interest and principal repayments",
        "FileName": "Receipt",
        "SkipRows": 4,
        "ColTypes": {
            "Rec. Dt":"date-%d/%m/%Y",
            "Rec. No":str,
            "GL. No. - GL Date":str,
            "Name":str,
            "Weight":float,
            "Prici. Rec":float,
            "Int. Rec.":float,
            "N/S Charge":float,
            "Total Amt":float,
            "ST":str,
            "Int. Calculated On":float,
            "No. of Days":int,
        },
        "Format":"xls",
        "DropRowsEnd":2,#Drop total rows and empty rows at the end
        "DerivedCols":{                
            "Method":"SplitCol",
            "SourceCols":["GL. No. - GL Date"],
            "TargetCols":["GL. No.","GL Date"],
            "TargetDataTypes":[str,"date-%d/%m/%Y"],
            "SplitMethod":"UseDelimiter",
            "DelimiterVal":"-",
            "KeepIndex":[0]
        },
        "PreProcessOps":[
            {"TrimCols":{"Cols":["Name"]}},
            {"ToUpper":{"Cols":["Name"]}},
            {
                "DerivedCols":
                {                
                    "Method":"SplitCol",
                    "SourceCols":["GL. No. - GL Date"],
                    "TargetCols":["GL. No.","GL Date"],
                    "TargetDataTypes":[str,"date-%d/%m/%Y"],
                    "SplitMethod":"UseDelimiter",
                    "DelimiterVal":"-",
                    "KeepIndex":[0]
                },
            }
        ]
    },
    "LoanDelivery":
    {
        "Merged": False,
        "Description":"Loan details",
        "SkipRows":4,
        "DropRowsEnd":2,#Drop total rows and empty rows at the end
        "ColTypes":{"Date":"date-%d-%m-%Y","GL. No":str,"Name":str,"Phone":str,"Items":str,"Weight":float,"Amt. Given":float,"Princ. Rec.":str,"Int. Rec.":str},
        "Format":"xls",
        "PreProcessOps":[
            {"TrimCols":{"Cols":["Name"]}},
            {"ToUpper":{"Cols":["Name"]}},
        ],  
    }
}
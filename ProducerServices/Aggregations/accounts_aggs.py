AccountsAggs={"CheckSumAccounts":[
        {
            "$unwind":
            {
                "path": "$Source"
            }
        },       
        {
            "$group":
            {
                "_id": {"Source":"$Source"},
                "CheckSumLoanAmount":
                {
                    "$sum": "$LoanAmount"
                },
                "CheckSumWeight":
                {
                    "$sum": "$CollWeight"
                },
                "CheckCount": 
                {
                    "$count": {}
                }
            }
        },
        {
            "$project":
            {
                "Source": "$_id.Source",
                "CheckSumLoanAmount": 1,
                "CheckSumWeight": 1,                
                "CheckCount": 1,
                "_id": 0
            }        
        }        
    ],
        "CheckCountLoanStatus":[
        {
            "$unwind":
            {
                "path": "$Source"
            }
        },
        {
            "$group":
            {
                "_id": 
                {
                    "LoanStatus": "$LoanStatus",
                    "Source": "$Source"
                },
                "CheckCount": {"$sum": 1}
            }
        },
        {
            "$project": 
            {
                "LoanStatus": "$_id.LoanStatus",
                "Source": "$_id.Source",
                "CheckCount": 1,
                "_id": 0
            }
        }
    ]
}
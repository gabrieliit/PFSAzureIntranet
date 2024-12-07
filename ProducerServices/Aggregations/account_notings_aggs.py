AccountNotingsAggs={
    "CheckSumAccountNotings":[
        {
            "$unwind": {
            "path": "$Source"
            }
        },
        {
            "$match": 
            {
                "NotingType": 
                {
                    "$in": 
                    [
                        "PrincDue",
                        "PrincRec",
                        "IntDue",
                        "IntRec",
                        "PendingDays",
                        "IntRate",
                        "PrincPerGm",
                        "CollVal",
                        "Notice1",
                        "Auction",
                        "Notice2"
                    ]
                }
            }
        },
        {
            "$addFields": 
            {
                "Amount": 
                {
                    "$cond":
                    { 
                        "if": 
                        { 
                            "$eq": ["$Amount", "nan"] 
                        }, 
                        "then": 0.0, 
                        "else":
                        {    
                            "$convert": 
                            {
                                "input": "$Amount",
                                "to": "double",
                                "onError": 0.0
                            }
                        }
                    }
                }
            }
        },
        {
            "$group": 
            {
                "_id": 
                {
                    "NotingType": "$NotingType",
                    "Source": "$Source"
                },
                "CheckSumAmount": 
                {
                    "$sum": "$Amount"
                },
                "CheckCount": 
                {
                    "$sum": 1
                }
            }
        },
        {
            "$project":
            {
                "Source": "$_id.Source",
                "NotingType": "$_id.NotingType",               
                "CheckSumAmount": 1,            
                "CheckCount": 1,
                "_id": 0
            }        
        }       
    ]
}
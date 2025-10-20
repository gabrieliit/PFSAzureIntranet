CustomersAggs={
    "CheckSumCustomers":
    [
        {
            "$unwind":{"path": "$Source"}
        },
        {
            "$group":
            {
                "_id": 
                {
                    "Source": "$Source"
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
                "CheckCount": 1,
                "_id": 0
            }        
        }     
    ] 
}
TransactionsAggs={
  "CheckSumTxns":[
    {
      "$unwind":
      {
        "path": "$Source"
      }
    },
    {
      "$group":
      {
        "_id": {"TxnType":"$TxnType","Source":"$Source"},
        "ChecksumAmount": {
          "$sum": "$Amount"
        },
        "CheckCount": {
          "$count": {}
        }
      }
    },
    {
      "$project":
        {
          "TxnType": "$_id.TxnType",
          "Source": "$_id.Source",
          "ChecksumAmount": 1,
          "CheckCount": 1,
          "_id": 0
        }        
    }
  ]
}
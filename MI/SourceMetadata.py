Sources = {
    "PendingLoan":
    {
        "Merged":False,
        "Description": "Loan details for all loans",
        "FileName":"PendingLoan",
        "SkipRows":4,
        "Format":"csv",
        #These should be valiadted against a data dictionary -TBD
        "ColNames":["SlNo","LoanDate","GLNo","Name","Phone","AmountGiven","IntRate","Weight","PrincRec","PrincBal","IntRec","IntBal","AgeDays","Scheme","Blank","DueDate","Address","NoticeType","NoticeDate","Notes"]
    },
    "LoanStatus":
    {
        "Merged":False,
        "Description":"Loan amounts and balances for pending loans",
        "FileName": "LoanStatus",
        "SkipRows": 3,
        "ColNames":["LoanDate","GLNo","Name","AmountGiven","IntBal","PrincBal"],
        "DerivedCols":["OverdueSince"],
        "Format":"xls"
    },
    "PendingLoan_30":
    {
        "Merged":False,
        "Description": "Loan details for loans with payments due more than 30 days",
        "FileName": "PendingLoan_30",
        "SkipRows": 4,
        "ColNames": ["LoanDate","GLNo","Name","AmountGiven","IntRate","Weight","PrincBal","IntRec","IntBal","TotalBal","AvgBalperGm","CollVal","Diff","RecUpto","PendingDays","Scheme","DueDate","Phone","NoticeType","NoticeDate","Notes"],
        "Format":"xls"
    },
    "Receipt":
    {
        "Merged":False,
        "Description": "Receipt details for interest and principal repayments",
        "FileName": "Receipt",
        "SkipRows": 4,
        "ColNames": ["RecDate","RecNo","GLNo","Name","Weight","PrincRec","IntRec","NSCharge","TotalRec","ST","Notional","IntDueDays"],
        "Format":"xls"
    },
    "Merged_Receipt_PendingLoan":
    {
        "Merged": True,
        "Description":"Merged Receipt and Pending Loans",
        "Sources":["PendingLoan","Receipt"],
        "MergeParams":{"MergeCols":["GLNo"],"MergeType":"left"},
        "ColNames":["GLNo", "Name", "Phone", "TxnDate", "Principal", "Interest", "TotalAmt", "Type","InLoanInv"]
    }

}
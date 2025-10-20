import pandas as pd

mi_run_params = {
    "CollRate": 6640.0,
    "Dataset": "24052024",
    "RepDate": pd.to_datetime('2024-05-24'),
    "TrendDatasets":["23102023","16042024"],
    "IncludeReports":["ExecSummary","Trends","LoanStatus","PendingLoan","PendingLoan_30","ReceiptTotal","Customer"]
}

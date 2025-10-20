import pandas as pd
from RunConfig import mi_run_params
from dateutil.relativedelta import relativedelta

MI_facts_metadata=\
{
    "LP01_No of unique GL":
    {
        "Domain":"Account",
        "MetricType":"Volume",
        "Source":["LoanStatus","PendingLoan_30","PendingLoan","Receipt"],
        "CalcType":"list_unique",
        "CalcAttributes":["GLNo"],
        "SourceAggType":"count_unique"
    },
    "LP02_Total Principal Given":
    {
        "Domain":"Portfolio",
        "MetricType":"Volume",
        "Source": ["LoanStatus", "PendingLoan_30", "PendingLoan"],
        "CalcAttributes":["AmountGiven"],
        "CalcType":"col_sum",
        "SourceAggType":"max"
    },
    "LP03_Total Interest Due":
    {
        "Domain":"Portfolio",
        "MetricType":"Recievable",
        "Source": ["LoanStatus", "PendingLoan_30"],
        "CalcAttributes": ["IntBal"],
        "CalcType": "col_sum",
        "SourceAggType": "max"
    },
    "LP04_Total Principal Outstanding":
    {
        "Domain":"Portfolio",
        "MetricType":"Receivable",
        "Source": ["LoanStatus", "PendingLoan_30", "PendingLoan"],
        "CalcAttributes": ["PrincBal"],
        "CalcType": "col_sum",
        "SourceAggType": "max"
    },
    "CB01_No of unique Cust Names":
    {
        "Domain":"Customer",
        "MetricType":"Volume",
        "Source": ["LoanStatus", "PendingLoan_30", "PendingLoan", "Receipt"],
        "CalcType": "list_unique",
        "CalcAttributes": ["Name"],
        "SourceAggType": "count_unique"
    },
    "LP05_Average Loan Amount":
    {
        "Domain":"Portfolio",
        "MetricType":"TxnStatistic",
        "Source": ["PendingLoan"],
        "CalcAttributes": ["AmountGiven"],
        "CalcType": "col_avg",
        "SourceAggType": "max"
    },
    "TXV01_Loans disbursed per day (15 week avg)":
    {
        "Domain":"Transaction",
        "MetricType":"Volume",
        "Source": ["LoanStatus", "PendingLoan_30", "PendingLoan", "Receipt"],
        "CalcType": "list_unique",
        "CalcAttributes": ["GLNo"],
        "SourceAggType": "count_unique",
        "Filters":[{"PartitionCol": "LoanDate", "Condition": ">","PartitionVal": pd.to_datetime(mi_run_params["RepDate"] - relativedelta(days=105))}], #Remove dependency on run config in this static defn. Add rep date as a column to the df in hte data load pre-process??
        "PostAggJobs":[{"JobType":"div","Denom":90}]
    },
    "CB02_No of unique Phone Nos":
    {
        "Domain":"Customer",
        "MetricType":"Volume",
        "Source": ["PendingLoan_30", "PendingLoan"],
        "CalcType": "list_unique",
        "CalcAttributes": ["Phone"],
        "SourceAggType": "count_unique"
    },
    "CRM08_Princ LTV":
    {
        "Domain":"Portfolio",
        "MetricType":"KRI",
        "Source": ["PendingLoan"],
        "CalcType": "ratio_col_sum",
        "CalcAttributes": {"Num":"PrincBal", "Denom":"CollVal"},
    },
    "LP06_AvgAgingDays":
    {
        "Domain":"Portfolio",
        "MetricType":"KRI",
        "Source": ["PendingLoan"],
        "CalcAttributes": ["AgeDays"],
        "CalcType": "col_avg"
    },
    "CRM03_% Overdue Loan balance":
    {
        "Domain": "Portfolio",
        "MetricType": "KRI",
        "Source": ["PendingLoan"],
        "CalcType": "partition_proportion",
        "CalcAttributes": ["PrincBal"],
        "Filters":[{"PartitionCol": "DueDate", "Condition": "<", "PartitionVal": pd.to_datetime(mi_run_params["RepDate"])}], #this value should be parametrised to make it static ratehr than run dependent. Add rep date as a column to the df in hte data load pre-process??
        "SourceAggType": "avg",
        "DisplayFormat":"{:.2f}%"
    },
    "CRM04_No of overdue loans":
    {
        "Domain": "Portfolio",
        "MetricType": "KRI",
        "Source": ["PendingLoan"],
        "CalcAttributes": ["GLNo"],
        "CalcType":"list_unique",
        "SourceAggType":"count_unique",
        "Filters": [{"PartitionCol": "DueDate", "Condition": "<", "PartitionVal": pd.to_datetime(mi_run_params["RepDate"])}], # this value should be parametrised to make it static ratehr than run dependent. Add rep date as a column to the df in hte data load pre-process??
    },
    "CRM05_No of loans on auction notice":
    {
        "Domain": "Portfolio",
        "MetricType": "KRI",
        "Source": ["PendingLoan"],
        "CalcType": "list_unique",
        "CalcAttributes": ["GLNo"],
        "SourceAggType": "count_unique",
        "Filters": [{"PartitionCol": "NoticeType", "Condition": "=", "PartitionVal": "Auction"}]# this value should be parametrised to make it static ratehr than run dependent
    },
    "CRM06_Avg. days to trigger Auction Notice ":
    {
        "Domain": "Portfolio",
        "MetricType": "KRI",
        "Source": ["PendingLoan"],
        "CalcType": "diff_of_cols",
        "CalcAttributes": {"Col1":"NoticeDate","Col2":"OverdueSince"},
        "Filters": [{"PartitionCol": "NoticeType", "Condition": "=", "PartitionVal": "Auction"}],# this value should be parametrised to make it static ratehr than run dependent
        "PostAggJobs":
        [
            {"JobType": "mean"},
            {"JobType":"time_delta_to_days"}
        ]
    },
    "CRM09_Auction Princ Balance Amount":
    {
        "Domain": "Portfolio",
        "MetricType": "KRI",
        "Source": ["PendingLoan"],
        "Filters": [{"PartitionCol": "NoticeType", "Condition": "=", "PartitionVal": "Auction"}],# this value should be parametrised to make it static ratehr than run dependent
        "CalcType":"col_sum",
        "CalcAttributes":["PrincBal"]
    },
    "CB03_No of unique Addresses":
    {
        "Domain": "Customer",
        "MetricType": "KRI",
        "Source": ["PendingLoan"],
        "CalcType": "list_unique",
        "CalcAttributes": ["Address"],
        "SourceAggType": "count_unique"
    },
    "LP06_Wtd Average Int Rate":
    {
        "Domain": "Portfolio",
        "MetricType": "TxnStatistic",
        "Source": ["PendingLoan_30"],
        "CalcType":"wtd_avg",
        "CalcAttributes":{"Vals":"IntRate","Weights":"PrincBal"},
    },
    "CRM01_Gross LTV":
    {
        "Domain": "Portfolio",
        "MetricType": "KRI",
        "Source": ["PendingLoan_30"],
        "CalcType": "ratio_col_sum",
        "CalcAttributes": {"Num": "TotalBal", "Denom": "CollVal"}
    },
    "CRM02_AvgPendingDays":
    {
        "Domain": "Portfolio",
        "MetricType": "KRI",
        "Source": ["PendingLoan_30"],
        "CalcType":"col_avg",
        "CalcAttributes":["PendingDays"]
    },
    "CRM07_Auction Loan Balance Amount":
    {
        "Domain": "Portfolio",
        "MetricType": "KRI",
        "Source": ["PendingLoan_30"],
        "Filters": [{"PartitionCol": "NoticeType", "Condition": "=", "PartitionVal": "Auction"}],
        "CalcType": "col_sum",
        "CalcAttributes": ["TotalBal"]
    },
    "LP08_Total Principal Returned Closed Loans":
    {
        "Domain": "Portfolio",
        "MetricType": "Volume",
        "Source": ["Receipt"],
        "CalcType":"col_sum",
        "CalcAttributes":["PrincRec"],
        "Filters":
        [
            {"PartitionCol": "InLoanInv", "Condition": "=", "PartitionVal": False},
            {"PartitionCol": "Type", "Condition": "!=", "PartitionVal": "OPN"},
        ],
    },
    "TXV02_Loans closed per day (15 week avg)":
    {
        "Domain": "Transaction",
        "MetricType": "Volume",
        "Source": ["Merged_Receipt_PendingLoan"],
        "Filters":
        [
            {"PartitionCol": "Date", "Condition": ">","PartitionVal": pd.to_datetime(mi_run_params["RepDate"] - relativedelta(days=105))},
            {"PartitionCol": "Type", "Condition": "=","PartitionVal": "CLS"}
        ],
        "CalcType":"list_unique",
        "CalcAttributes":["GLNo"],
        "SourceAggType":"count_unique",
        "PostAggJobs":[{"JobType":"div","Denom":90}]
    },
    "TXV03_Loans int_payment per day (15 week avg)":
    {
        "Domain": "Transaction",
        "MetricType": "Volume",
        "Source": ["Merged_Receipt_PendingLoan"],
        "Filters":
            [
                {"PartitionCol": "Date", "Condition": ">",
                 "PartitionVal": pd.to_datetime(mi_run_params["RepDate"] - relativedelta(days=105))},
                {"PartitionCol": "Type", "Condition": "=", "PartitionVal": "INT"}
            ],
        "CalcType": "list_unique",
        "CalcAttributes":["GLNo"],
        "SourceAggType": "count_unique",
        "PostAggJobs": [{"JobType": "div", "Denom": 90}]
    }
}


from MI_Class_defs import MetricFactory
import pandas as pd
from MI_metadata import MI_facts_metadata
from Doc_Architect import Document

def generate_trends_report(metrics,run_config):
    mf=MetricFactory(metrics, run_config)
    mf.calc_metrics()
    cols=["Metric", "Domain"]
    datasets=run_config["TrendDatasets"]+[run_config["Dataset"]]
    cols=cols+[f"Val_{dataset}" for dataset in datasets]+[f"inc_{datasets[i]}_{datasets[i+1]}" for i in range(len(datasets)-1)]
    cols+=["Comment"]
    output_df= pd.DataFrame(columns=cols)
    for metric in metrics:
        row={"Metric":metric,"Domain":MI_facts_metadata[metric]["Domain"],"Type":MI_facts_metadata[metric]["MetricType"]}
        i=0
        for dataset in datasets:
            job_outputs=mf.job_q[dataset]["agg_jobs"][f"sa_job_{dataset}_{metric}"]["job_outputs"]
            format_str = job_outputs["DisplayFormat"]
            if format_str:  # convert to formatted str if DisplayFormat is specified
                display_result=format_str.format(job_outputs["calc_result"])
                row[f"Val_{dataset}"]=display_result
            else:
                row[f"Val_{dataset}"]=job_outputs["calc_result"]
            if i>0:
                v0=mf.job_q[datasets[i-1]]["agg_jobs"][f"sa_job_{datasets[i-1]}_{metric}"]["job_outputs"]["calc_result"]
                v1=mf.job_q[datasets[i]]["agg_jobs"][f"sa_job_{datasets[i]}_{metric}"]["job_outputs"]["calc_result"]
                try:
                    row[f"inc_{datasets[i-1]}_{datasets[i]}"]=v1/v0-1
                except TypeError:
                    pass
                except ZeroDivisionError:
                    row[f"inc_{datasets[i-1]}_{datasets[i]}"]=0.0
            i+=1
            try:
                row["Comment"]=job_outputs["comment"]
            except KeyError:
                row["Comment"]=""
        output_df=pd.concat([output_df,pd.DataFrame([row])])
    for i in range(len(datasets)-1):#format increment columns as percentages
        output_df[f"inc_{datasets[i]}_{datasets[i+1]}"]=output_df[f"inc_{datasets[i]}_{datasets[i+1]}"].apply("{:.2%}".format)
    output_df.set_index("Metric",inplace=True) #Convert to Metric to INdex for use with Document class
    report=Document(name="Report_" + run_config["Dataset"] + "_Trends.pdf", folder= 'MI/Reports/' + run_config["Dataset"])
    pg1=report.add_page()
    pg1.add_section("table","Trends - Volume Metrics",content=output_df[output_df["Type"]=="Volume"].drop(columns=["Type"]).sort_values(by="Domain"),row_label="Metric",content_font_size=7,table_height_sf=3.0) #table height scaling factors are used to adjust height to compensate for text within a cell wrappin to multiple lines
    pg2=report.add_page()
    pg2.add_section("table", "Trends - KRI Metrics", content=output_df[output_df["Type"] == "KRI"].drop(columns=["Type"]).sort_values(by="Domain"),row_label="Metric", content_font_size=7, table_height_sf=2.0)
    pg3 = report.add_page()
    pg3.add_section("table", "Trends - Transaction averages",content=output_df[output_df["Type"] == "TxnStatistic"].drop(columns=["Type"]).sort_values(by="Domain"),row_label="Metric", content_font_size=7, table_height_sf=2.0)
    report.render_doc()
    return output_df


if __name__=="__main__":
    #   Perform Trend analysis
    metrics=\
    [
        "LP01_No of unique GL",
        "LP02_Total Principal Given",
        "LP03_Total Interest Due",
        "LP04_Total Principal Outstanding",
        "CB01_No of unique Cust Names",
        "LP05_Average Loan Amount",
        "TXV01_Loans disbursed per day (15 week avg)",
        "CB02_No of unique Phone Nos",
        "CRM08_Princ LTV",
        "LP06_AvgAgingDays",
        "CRM03_% Overdue Loan balance",
        "CRM04_No of overdue loans",
        "CRM05_No of loans on auction notice",
        "CRM06_Avg. days to trigger Auction Notice ",
        "CRM09_Auction Princ Balance Amount",
        "CB03_No of unique Addresses",
        "LP06_Wtd Average Int Rate",
        "CRM01_Gross LTV",
        "CRM02_AvgPendingDays",
        "CRM07_Auction Loan Balance Amount",
        "LP08_Total Principal Returned Closed Loans",
        "TXV02_Loans closed per day (15 week avg)",
        "TXV03_Loans int_payment per day (15 week avg)"
    ]
    mi_run_params = {
        "CollRate": 6705.0,
        "Dataset": "24052024",
        "RepDate": pd.to_datetime('2024-05-24'),
        "TrendDatasets":["23102023"]#historical dataset for trend analysis
    }

    result=generate_trends_report(metrics,mi_run_params)
    print(result)

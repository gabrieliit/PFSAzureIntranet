# import python packages
import pandas as pd
import os
# main.py
from LoanStatusReport import generate_loan_status_report
from PendingLoan30Report import generate_pending_loan_30_report
from PendingLoanReport import generate_pending_loan_report
from ReceiptsTotal import generate_receipts_total_report
from CutomerReport import generate_customer_report
from Doc_Architect import Document
from RunConfig import mi_run_params
from MI_Class_defs import MetricFactory
from DataServices.Extract.source_metadata import Sources
from MI_metadata import MI_facts_metadata
from Trends import generate_trends_report
def get_filename(source, dataset):
    return Sources[source]["FileName"]+f"_{dataset}."+ Sources[source]["Format"]

def main():
    rep_date=pd.to_datetime(mi_run_params["RepDate"]).strftime('%d%m%Y')
    loan_status_summary=generate_loan_status_report(mi_run_params["Dataset"], rep_date=rep_date, source_name="LoanStatus")
    #MI_items=["No of unique GL","Total Principle Given","Total Interest Due","Total Principle Due","No of unique Cust Names","Average Loan Amount","Loans disbursed per day 15 week avg","Wtd Average Int Rate"]
    summary_df=pd.DataFrame()
    summary_df=add_to_summary_df(summary_df,loan_status_summary,Sources["LoanStatus"]["FileName"])
    pending_loan_30_summary,pie_chart_ir_path=generate_pending_loan_30_report(mi_run_params["Dataset"], rep_date=pd.to_datetime(mi_run_params["RepDate"]), source_file=get_filename("PendingLoan_30",mi_run_params["Dataset"]))
    summary_df=add_to_summary_df(summary_df,pending_loan_30_summary,"PendingLoan_30.xls")
    pending_loan_summary,LTV_pie_path=generate_pending_loan_report(mi_run_params["Dataset"], rep_date=pd.to_datetime(mi_run_params["RepDate"]), source_file=get_filename("PendingLoan",mi_run_params["Dataset"]), coll_rate=mi_run_params["CollRate"])
    summary_df=add_to_summary_df(summary_df,pending_loan_summary,"PendingLoan.xls")
    generate_receipts_total_report(mi_run_params["Dataset"], rep_date=pd.to_datetime(mi_run_params["RepDate"]), source_file=get_filename("Receipt",mi_run_params["Dataset"]))
    customer_report_summary=generate_customer_report(mi_run_params["Dataset"], rep_date=pd.to_datetime(mi_run_params["RepDate"]), receipts_source=get_filename("Receipt",mi_run_params["Dataset"]), pending_loan_source=get_filename("PendingLoan",mi_run_params["Dataset"]))
    summary_df = add_to_summary_df(summary_df, customer_report_summary, "TotRec&Loans.join")
    summary_df=summary_df.sort_index()
    report=Document(name="Report_" + mi_run_params["Dataset"] + "_ExecSummary.pdf", folder= 'MI/Reports/' + mi_run_params["Dataset"])

    pg1=report.add_page()
    pg1.add_section("table","Exec Summary",content=summary_df,row_label="MI Item",content_font_size=7,table_height_sf=1.5)
    pg2=report.add_page(n_sections=2)
    pg2.add_section("image","Distribution by Int Rate -Pending_Loans_30.XLS",content=pie_chart_ir_path,content_font_size=6)
    pg2.add_section("image", "Distribution by LTV -Pending_Loans.XLS", content=LTV_pie_path,
                    content_font_size=6)
    report.render_doc()
    #   Perform Trend analysis
    metrics=MI_facts_metadata.keys()
    generate_trends_report(metrics,mi_run_params)


def add_to_summary_df(summary_df, new_col, col_name):
    "Append col to an existing df containing subset of rows. Fill NA where rows are now present in new col"
    summary_df[col_name]="NA"
    new_rows={}
    new_index=[]
    for col in summary_df.columns:
        new_rows[col]=[]
    for index,val in new_col.items():
        if index in summary_df.index:
            summary_df.at[index,col_name]=val
        else:
            new_index.append(index)
            for col in summary_df.columns:
                if col==col_name:
                    new_rows[col].append(val)
                else:
                    new_rows[col].append("NA")
    new_rows_df=pd.DataFrame(new_rows,index=new_index)
    summary_df=pd.concat([summary_df,new_rows_df])
    return summary_df

if __name__ == "__main__":
    main()

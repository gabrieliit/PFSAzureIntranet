# PendignLoanreport.py
import datetime

import pandas as pd
from summary_stats import generate_6m_rolling_summary_stats,partition_summary_stats
from histogram_plot import generate_histograms,generate_gr_st_histogram
from pie_chart import generate_pie_chart
from Doc_Architect import Document
from dateutil.relativedelta import relativedelta
import numpy as np
import os

def generate_pending_loan_30_report(dataset,rep_date,source_file):
    # Replace 'your_file.csv' with your actual CSV file path
    file_path = os.path.join('MI/Data',dataset,source_file)
    output_folder='MI/Reports/' + dataset +'/PendingLoan_30'
    report_folder = 'MI/Reports/' + dataset
    # Read Excel file into a DataFrame
    df = pd.read_excel(file_path, skiprows=4)
    df.columns=["LoanDate","GLNo","Name","AmountGiven","IntRate","Weight","PrincBal","IntRec","IntBal","TotalBal","AvgBalperGm","CollVal","Diff","RecUpto","PendingDays","Scheme","DueDate","Phone","NoticeType","NoticeDate","Notes"]
    cols_to_drop=["Weight","AvgBalperGm","RecUpto","Notes"]
    df=df.drop(columns=cols_to_drop)
    # Drop rows with null values
    df = df.dropna()
    # Convert the date column to datetime format
    date_cols=["LoanDate","DueDate"]
    for col in date_cols:
        df[col]=pd.to_datetime(df[col],format="mixed",dayfirst=True)
    sum_tot_bal=df["TotalBal"].sum()
    df["wtdPendingDays"]=df["PendingDays"]*df["TotalBal"]/sum_tot_bal
    df["wtdPendingDays"]*=df["wtdPendingDays"].count()#make comparable to unweighted individual entries
    df["RemTenure"] =df["DueDate"]-rep_date
    df["RemTenure"]=df["RemTenure"].apply(lambda x:x.days)
    # Assuming the first column is the date column and the rest are numeric columns
    summary_cols=["AmountGiven","IntRate","PrincBal","TotalBal","CollVal","PendingDays","wtdPendingDays"]
    summary_stat_df=df[summary_cols]
    # Convert to numeric
    for col in summary_stat_df:
        pd.to_numeric(df[col], errors='coerce')
    # Generate Summary stats table
    summary_stats = summary_stat_df.describe()
    tot_row=summary_stats.loc["count"]*summary_stats.loc["mean"]
    tot_row.name="Total"
    tot_row_df = pd.DataFrame(tot_row).transpose()
    summary_stats= pd.concat([summary_stats,tot_row_df])
    decimal_points=1
    for col in summary_stats.columns:
        summary_stats[col]=summary_stats[col].round(decimal_points)
    #create a document object
    report=Document(name="Report_"+dataset+"_PendingLoans_30.pdf",folder=report_folder)
    #define sections and pages
    #page 1 - print per loan stats table
    page1 = report.add_page()
    page1.add_section("table",f"Summary Stats per Loan (Full history)",content=summary_stats,row_label="Statistic",content_font_size=8)
    #page 2 -  lending behaviour
    # Generate bins
    lending_param_cols=["AmountGiven","IntRate","CollVal"]
    date_column="LoanDate"
    min_date=df[date_column].min()
    max_date=df[date_column].max()
    output_paths,cols,rolling_stats=generate_6m_rolling_summary_stats(df[[date_column]+lending_param_cols],min_date, max_date,output_folder)
    #Define page2 - pages 3 per loan rolling stats line plots for Amt Given, Principal Due and Interest Due cols
    #Add page
    page2 = report.add_page(n_sections=3)
    #add a section on page for each plot
    i=0
    for col in cols:
        page2.add_section("image",f"Per Loan Stats {col}",content=output_paths[i])
        i+=1
    #page3 - Aggregate counts and sums per month
    # Hist 1 - Tot_Bal by month
    df_TotBal_by_Month=df[["LoanDate","GLNo","TotalBal"]]
    count_columns = ['GLNo']
    val_cols=["TotalBal"]
    hist1_image_path = generate_histograms(df_TotBal_by_Month, output_folder,"hist1",count_columns,val_cols,"Months")
    # Hist 2 - Tot_Bal by pending days
    df_TotBal_by_PD=df[["PendingDays","GLNo","TotalBal"]]
    df_TotBal_by_PD["PendingDays"]=pd.to_numeric(df_TotBal_by_PD["PendingDays"])
    hist2_image_path = generate_histograms(df_TotBal_by_PD, output_folder,"hist2",count_columns,val_cols,create_bin_type="Equal_partitions",nbins=30,bar_width="vh",xlabel="Pending Days")
    # Define page 3
    page3=report.add_page(n_sections=2)
    page3.add_section("image","Aggregate counts and amounts per month",content=hist1_image_path,content_font_size=6,shsf=1.0,swsf=1.0)
    page3.add_section("image","Aggregate counts and amounts by pending days",content=hist2_image_path,content_font_size=6,shsf=1.0,swsf=1.0)
    #page4 -Aggregate counts and sums by remaining tenture
    #Bal by tenure
    df_TotBal_by_Tenure = df[["RemTenure", "GLNo", "TotalBal"]]
    df_TotBal_by_Tenure["RemTenure"]=pd.to_numeric(df_TotBal_by_Tenure["RemTenure"])
    hist3_image_path = generate_histograms(df_TotBal_by_Tenure, output_folder,"hist3",count_columns,val_cols,create_bin_type="Equal_partitions",nbins=30,bar_width="vh",xlabel="Tenure Days")
    # Define page 4
    page4=report.add_page(n_sections=2)
    page4.add_section("image","Aggregate counts and amounts by remaining tenure",content=hist3_image_path,content_font_size=6,shsf=1.0,swsf=1.0)
    # Auctioned loans - Aggregate balance and counts by notice trigger period and notice tenure
    #filter out auctioned loans
    df_auctioned_raw=df[df["NoticeType"]=="Auction"]
    df_auctioned=df_auctioned_raw[summary_cols]
    #create summary stats fo auctioned loands
    summary_stats=df_auctioned.describe()
    tot_row=summary_stats.loc["count"]*summary_stats.loc["mean"]
    tot_row.name="Total"
    tot_row_df = pd.DataFrame(tot_row).transpose()
    summary_stats= pd.concat([summary_stats,tot_row_df])
    decimal_points=1
    for col in summary_stats.columns:
        summary_stats[col]=summary_stats[col].round(decimal_points)
    #create summary stats for  loans on notice
    #filter out auctioned loans
    df_notice_raw=df[df["NoticeType"]=="Notice 1"]
    df_notice=df_notice_raw[summary_cols]
    summary_stats_notice=df_notice.describe()
    tot_row=summary_stats.loc["count"]*summary_stats.loc["mean"]
    tot_row.name="Total"
    tot_row_df = pd.DataFrame(tot_row).transpose()
    summary_stats_notice= pd.concat([summary_stats_notice,tot_row_df])
    decimal_points=1
    for col in summary_stats.columns:
        summary_stats_notice[col]=summary_stats_notice[col].round(decimal_points)
    # page 5 - print per loan stats table
    page5 = report.add_page(n_sections=2)
    page5.add_section("table", f"Summary Stats per Auctioned Loan (Full history)", content=summary_stats, row_label="Statistic")
    page5.add_section("table", f"Summary Stats per Notice Loan (Full history)", content=summary_stats_notice, row_label="Statistic")
    #Aggregate balance and counts by auction trigger tenure
    df_auctioned_by_trigger_period=df_auctioned_raw[["LoanDate","NoticeDate", "GLNo", "TotalBal","PendingDays"]]
    df_auctioned_by_trigger_period["NoticeDate"]=pd.to_datetime(df_auctioned_by_trigger_period["NoticeDate"],format="mixed",dayfirst=True)
    df_auctioned_by_trigger_period["PendingDays"]=pd.to_timedelta(pd.to_numeric(df_auctioned_by_trigger_period["PendingDays"]),unit="D")
    df_auctioned_by_trigger_period["PendingTriggerDate"]=rep_date-df_auctioned_by_trigger_period["PendingDays"]+pd.to_timedelta(1)
    df_auctioned_by_trigger_period["condition"]=df_auctioned_by_trigger_period["PendingTriggerDate"]>df_auctioned_by_trigger_period["LoanDate"]
    df_auctioned_by_trigger_period["TriggerPeriod"]=""
    df_auctioned_by_trigger_period.loc[df_auctioned_by_trigger_period["condition"],"TriggerPeriod"]=df_auctioned_by_trigger_period.loc[df_auctioned_by_trigger_period["condition"],"PendingTriggerDate"]
    df_auctioned_by_trigger_period.loc[~df_auctioned_by_trigger_period["condition"], "TriggerPeriod"] = df_auctioned_by_trigger_period.loc[~df_auctioned_by_trigger_period["condition"],"LoanDate"]
    df_auctioned_by_trigger_period["TriggerPeriod"]=df_auctioned_by_trigger_period["NoticeDate"]- df_auctioned_by_trigger_period["TriggerPeriod"]
    df_auctioned_by_trigger_period["TriggerPeriod"]=df_auctioned_by_trigger_period["TriggerPeriod"].apply(lambda x: x.days)
    # print histograms Aggregate balance and counts by auction trigger tenure and remaining tenure
    df_auctioned_by_trigger_period=df_auctioned_by_trigger_period[["TriggerPeriod","GLNo", "TotalBal"]]
    hist4_image_path=generate_histograms(df_auctioned_by_trigger_period,output_folder,"hist4",["GLNo"],["TotalBal"],create_bin_type="Equal_partitions",nbins=30,bar_width="vh",xlabel="Delinquency (days) before Auction Notice generated")
    df_auctioned_by_rem_tenure = df_auctioned_raw[["DueDate", "GLNo", "TotalBal"]]
    df_auctioned_by_rem_tenure["RemTenure"]=df_auctioned_by_rem_tenure["DueDate"]-rep_date
    df_auctioned_by_rem_tenure["RemTenure"]=df_auctioned_by_rem_tenure["RemTenure"].apply(lambda x:x.days)
    df_auctioned_by_rem_tenure = df_auctioned_by_rem_tenure[["RemTenure", "GLNo", "TotalBal"]]
    hist5_image_path=generate_histograms(df_auctioned_by_rem_tenure,output_folder,"hist5",["GLNo"],["TotalBal"],create_bin_type="Equal_partitions",nbins=30,bar_width="vh",xlabel="Remaining Tenure (Days)")
    # page 6 - print Aggregate balance and counts by notice trigger period and notice tenure
    page6=report.add_page(n_sections=2)
    page6.add_section("image"," Aggregate balance and counts by notice triggering period",content=hist4_image_path,content_font_size=6)
    page6.add_section("image"," Aggregate balance and counts by remaining tenure",content=hist5_image_path,content_font_size=6)
    #page7 - Loan schemes analyis
    pie_1_image_path=generate_pie_chart(df[["Scheme","TotalBal"]],output_folder,"pie1","Scheme","TotalBal")
    df_total_bal_by_scheme=df[["Scheme","LoanDate","IntRate","TotalBal","DueDate","PendingDays"]]
    df_total_bal_by_scheme["LoanAge"]=rep_date-df["LoanDate"]
    df_total_bal_by_scheme["LoanAge"]=df_total_bal_by_scheme["LoanAge"].apply(lambda x:x.days)
    df_total_bal_by_scheme["RemTenure"]=df["DueDate"]-rep_date
    df_total_bal_by_scheme["RemTenure"]=df_total_bal_by_scheme["RemTenure"].apply(lambda x:x.days)
    summary_stats_by_scheme=partition_summary_stats(df_total_bal_by_scheme,val_cols=["IntRate","TotalBal","PendingDays","LoanAge","RemTenure"],partition_col="Scheme",calc_incr=True,stats_to_drop=["count","mean","std"])
    time_attribs=["PendingDays","LoanAge","RemTenure"]
    hist_6_image_path=generate_gr_st_histogram(summary_stats_by_scheme[time_attribs+["Scheme","Stat"]],output_folder,"hist6",group_col="Scheme",stack_col="Stat",val_cols=time_attribs,ylabel="Days")
    page7=report.add_page(n_sections=2)
    page7.add_section("image", "Total Balance by Scheme", content=pie_1_image_path, content_font_size=6)
    page7.add_section("image","Total Balance by Scheme - duration attributes",content=hist_6_image_path,content_font_size=6)
    #page 8; Contd
    page8=report.add_page(n_sections=2)
    hist_7_image_path = generate_gr_st_histogram(summary_stats_by_scheme[["TotalBal"] + ["Scheme", "Stat"]],
                                                 output_folder, "hist7", group_col="Scheme", stack_col="Stat",
                                                 val_cols=["TotalBal"], ylabel="Amount",unit_scale="Lakhs")
    hist_8_image_path = generate_gr_st_histogram(summary_stats_by_scheme[["IntRate"] + ["Scheme", "Stat"]],
                                                 output_folder, "hist8", group_col="Scheme", stack_col="Stat",
                                                 val_cols=["IntRate"], ylabel="Int Rate",)
    page8.add_section("image","Total Balance by Scheme - Princ amount",content=hist_7_image_path,content_font_size=6)
    page8.add_section("image", "Total Balance by Scheme - Int Rate", content=hist_8_image_path, content_font_size=6)
    # print and save doc
    report.render_doc()
    output_df=\
    {
        "LP01_No of unique GL":len(df["GLNo"].unique()),
        "LP02_Total Principal Given":df["AmountGiven"].sum(),
        "LP03_Total Interest Due":df["IntBal"].sum(),
        "LP04_Total Principal Due":df["PrincBal"].sum(),
        "CB01_No of unique Cust Names":len(df["Name"].unique()),
        "CB02_No of unique Phone Nos":len(df["Phone"].unique()),
        "LP05_Average Loan Amount": df["AmountGiven"].sum()/len(df["GLNo"].unique()),
        "TXV01_Loans disbursed per day (15 week avg)":len(df[df["LoanDate"]>(rep_date-relativedelta(days=105))]["GLNo"].unique())/90,
        "LP06_Wtd Average Int Rate":np.sum(df["PrincBal"]*df["IntRate"])/df["PrincBal"].sum(),
        "CRM01_Gross LTV":df["TotalBal"].sum()/df["CollVal"].sum(),
        "CRM02_AvgPendingDays":df["PendingDays"].mean(),
        "CRM03_% Overdue Loan balance":df[df["DueDate"]<rep_date]["TotalBal"].sum()/df["TotalBal"].sum()*100,
        "CRM04_No of overdue loans":len(df[df["DueDate"]<rep_date]["TotalBal"]),
        "CRM05_No of loans on auction notice":len(df[df["NoticeType"]=="Auction"]),
        "CRM06_Avg. days to trigger Auction Notice":(pd.to_datetime(df[df["NoticeType"]=="Auction"]["NoticeDate"],format="mixed",dayfirst=True)-(rep_date-pd.to_timedelta(df[df["NoticeType"]=="Auction"]["PendingDays"],unit="d"))).mean().total_seconds()/86400,
        "CRM07_Auction Loan Balance Amount":df[df["NoticeType"]=="Auction"]["TotalBal"].sum(),
        "CRM09_Auction Princ Balance Amount": df[df["NoticeType"] == "Auction"]["PrincBal"].sum()
    }
    pie_2_image_path = generate_pie_chart(df[["IntRate", "TotalBal"]], output_folder, "pie2", "IntRate", "TotalBal")
    return output_df,pie_2_image_path

if __name__=="__main__":
    generate_pending_loan_30_report("Apr24", rep_date=pd.to_datetime('2024-04-16'))


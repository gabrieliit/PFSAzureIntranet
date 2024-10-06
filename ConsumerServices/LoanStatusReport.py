# main.py
import pandas as pd
from datetime import datetime
from summary_stats import generate_6m_rolling_summary_stats
from histogram_plot import generate_histograms
from Doc_Architect import Document
from dateutil.relativedelta import relativedelta
import os
from MI_Class_defs import DataSource
from DataServices.Extract.source_metadata import Sources
from RunConfig import mi_run_params

def generate_loan_status_report(dataset,rep_date,source_name):
    # Convert to datetime object
    rep_date = datetime.strptime(rep_date, '%d%m%Y')
    # Replace 'your_file.csv' with your actual CSV file path
    input_folder = os.path.join('MI/Data',dataset)
    output_folder='MI/Reports/' + dataset +'/Loan_status'
    report_folder = 'MI/Reports/' + dataset
    # Read Excel file into a DataFrame
    src_Loan_Status=DataSource(input_folder,source_name,mi_run_params)
    src_Loan_Status.load_data(skiprows=Sources[source_name]["SkipRows"])
    df = src_Loan_Status.df #for backward compatibility. will be refactored in future
    df.columns=["LoanDate","GLNo","Name","AmountGiven","IntBal","PrincBal"]
    # Drop rows with null values
    df = df.dropna()
    # Convert the date column to datetime format
    date_column = df.columns[0]
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    # Assuming the first column is the date column and the rest are numeric columns
    val_columns=df.columns[1:]
    # Drop non-numeric columns from the list of amount columns
    amount_columns_numeric = [col for col in val_columns if pd.to_numeric(df[col], errors='coerce').notnull().all()]
    # Select only numeric columns along with 'YearMonth'
    df_numeric = df[amount_columns_numeric]
    # Generate Summary stats table
    summary_stats = df_numeric.describe()
    tot_row=summary_stats.loc["count"]*summary_stats.loc["mean"]
    tot_row.name="Total"
    tot_row_df = pd.DataFrame(tot_row).transpose()
    summary_stats= pd.concat([summary_stats,tot_row_df])
    #create a document object
    report=Document(name="Report_"+dataset+"_LoanStatus.pdf",folder=report_folder)
    #define sections and pages
    #page 1 - print per loan stats table
    page1 = report.add_page()
    page1.add_section("table",f"Summary Stats per Loan (Full history)",content=summary_stats,row_label="Statistic")
    #page 2 -  per loan stats rolling 6m plots
    # Generate bins
    min_date=df[date_column].min()
    max_date=df[date_column].max()
    output_paths,cols,rolling_stats=generate_6m_rolling_summary_stats(df[[date_column]+amount_columns_numeric],min_date, max_date,output_folder)
    #Define page2 - pages 3 per loan rolling stats line plots for Amt Given, Principal Due and Interest Due cols
    #Add page
    page2 = report.add_page(n_sections=3)
    #add a section on page for each plot
    i=0
    for col in cols:
        page2.add_section("image",f"Per Loan Stats {col}",content=output_paths[i])
        i+=1
    #page3 - Aggregate counts and sums per month
    # Generate aggregates histogram and get the path to the histogram image
    count_columns = ['GLNo']
    val_cols=["AmountGiven","IntBal","PrincBal"]
    histogram_image_path = generate_histograms(df, output_folder,"hist",count_columns,val_cols,"Months")
    page3=report.add_page(n_sections=1)
    page3.add_section("image","Aggregate counts and amounts per month",content=histogram_image_path)
    report.render_doc()
    output_df=\
    {
        "LP01_No of unique GL":len(df["GLNo"].unique()),
        "LP02_Total Principal Given":df["AmountGiven"].sum(),
        "LP03_Total Interest Due":df["IntBal"].sum(),
        "LP04_Total Principal Due":df["PrincBal"].sum(),
        "CB01_No of unique Cust Names":len(df["Name"].unique()),
        "LP05_Average Loan Amount": df["AmountGiven"].sum()/len(df["GLNo"].unique()),
        "TXV01_Loans disbursed per day (15 week avg)":len(df[df[date_column]>(rep_date-relativedelta(days=105))]["GLNo"].unique())/90
    }
    return output_df

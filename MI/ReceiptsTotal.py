#imports
import pandas as pd
from summary_stats import generate_6m_rolling_summary_stats,partition_summary_stats
from histogram_plot import generate_histograms,generate_gr_st_histogram
from Doc_Architect import Document
import os


def generate_receipts_total_report(dataset, rep_date,source_file):
    # Replace 'your_file.csv' with your actual CSV file path
    file_path = os.path.join('MI/Data',dataset,source_file)
    output_folder='MI/Reports/' + dataset +'/ReceiptTotal'
    report_folder = 'MI/Reports/' + dataset
    rep_date=rep_date

    # Read Excel file into a DataFrame
    df = pd.read_excel(file_path, skiprows=4)
    df.columns=["RecDate","RecNo","GLNo","Name","Weight","PrincRec","IntRec","NSCharge","TotalRec","ST","Notional","IntDueDays"]
    cols_to_drop=["Weight"]
    df=df.drop(columns=cols_to_drop)
    df["ST"]=df["ST"].fillna("INT")
    # Drop rows with null values
    df = df.dropna()
    # Convert the date column to datetime format
    date_cols=["RecDate"]
    df[date_cols]=df[date_cols].apply(pd.to_datetime)
    #create a document object
    report=Document(name="Report_"+dataset+"_ReceiptsTotal.pdf",folder=report_folder)
    #define sections and pages
    #page 1 - print per loan stats table
    summary_cols=["PrincRec","IntRec","IntDueDays"]
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
    page1 = report.add_page()
    page1.add_section("table",f"Summary Stats per receipt (Full history)",content=summary_stats,row_label="Statistic",content_font_size=8)
    #page 2 -  lending behaviour
    # Generate bins
    date_column="RecDate"
    min_date=df[date_column].min()
    max_date=df[date_column].max()
    output_paths,cols,rolling_stats=generate_6m_rolling_summary_stats(df[[date_column]+summary_cols],min_date, max_date,output_folder,drop_stats=["max"])
    page2 = report.add_page(n_sections=3)
    #add a section on page for each plot
    i=0
    for col in cols:
        page2.add_section("image",f"Per Receipt Stats {col}",content=output_paths[i])
        i+=1
    #page3 - Aggregate counts and sums per month
    # Hist 1 - Tot_Bal by month
    df_TotBal_by_Month=df[["RecDate","GLNo","TotalRec"]]
    count_columns = ['GLNo']
    val_cols=["TotalRec"]
    hist1_image_path = generate_histograms(df_TotBal_by_Month, output_folder,"hist1",count_columns,val_cols,"Months")
    # Define page 3
    page3=report.add_page(n_sections=1)
    page3.add_section("image","Aggregate counts and amounts received per month",content=hist1_image_path,content_font_size=6,shsf=0.7,swsf=1.0)

    # print and save doc
    report.render_doc()

if __name__=="__main__":
    generate_receipts_total_report("Oct23", rep_date=pd.to_datetime('2023-10-23'))
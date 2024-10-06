#imports
import pandas as pd
from histogram_plot import generate_gr_st_histogram
from Doc_Architect import Document
from pie_chart import generate_pie_chart
from dateutil.relativedelta import relativedelta
import os

def generate_customer_report(dataset, rep_date, receipts_source,pending_loan_source):
    # Read Receipts data
    file_path = os.path.join('MI/Data',dataset,receipts_source)
    output_folder='MI/Reports/' + dataset +'/CustomerView'
    report_folder = 'MI/Reports/' + dataset
    # Read Excel file into a DataFrame
    df = pd.read_excel(file_path, skiprows=4)
    df.columns=["RecDate","RecNo","GLNo","Name","Weight","PrincRec","IntRec","NSCharge","TotalRec","ST","Notional","IntPeriod"]
    cols_to_drop=["Weight"]
    df=df.drop(columns=cols_to_drop)
    df["ST"]=df["ST"].fillna("INT")
    # Drop rows with null values
    df_receipts = df.dropna()
    df_receipts[["GLNo","LoanDate"]]=df_receipts["GLNo"].str.split('-',expand=True)
    # Convert the date column to datetime format
    df_receipts["LoanDate"]=df_receipts["LoanDate"].apply(pd.to_datetime)
    df_receipts["RecDate"] = df_receipts["RecDate"].apply(pd.to_datetime)
    # Read loans data
    # Replace 'your_file.csv' with your actual CSV file path
    file_path = os.path.join('MI/Data',dataset,pending_loan_source)
    # Read Excel file into a DataFrame
    df = pd.read_csv(file_path, skiprows=3)
    df.columns=["SlNo","LoanDate","GLNo","Name","Phone","AmountGiven","IntRate","Weight","Principal","PrincBal","Interest","IntBal","AgeDays","Scheme","Blank","DueDate","Address","NoticeType","NoticeDate","Notes"]
    coll_rate=6705
    df["CollVal"]=coll_rate*df["Weight"]
    cols_to_drop=["SlNo","Notes","Blank"]
    df=df.drop(columns=cols_to_drop)
    # Drop rows with null values
    df = df.dropna()
    # Convert the date column to datetime format
    df["LoanDate"]=df["LoanDate"].apply(pd.to_datetime)
    df["DueDate"] = df["DueDate"].apply(pd.to_datetime)
    df_loans=df
    #add phone number to df_receipts by joining with df_loans
    df_receipts=pd.merge(df_receipts,df_loans[["GLNo","Phone"]],left_on="GLNo",right_on="GLNo",how="left")
    #create a txns dataframe
    #add loan opening to txns
    df_open=df_loans[["GLNo","Name","Phone","LoanDate","AmountGiven"]]
    df_open=df_open.rename(columns={"LoanDate":"TxnDate","AmountGiven":"Principal"})
    df_open["Type"]="OPN"
    df_open["Interest"]=0.0
    df_open["TotalAmt"]=df_open["Principal"]
    #df_txns=df_txns.rename({"LoanDate":"Date","AmountGiven":"Amount"})
    df_txns=pd.DataFrame(columns=["GLNo","Name","Phone","TxnDate","Principal","Interest","TotalAmt","Type"])
    df_txns=pd.concat([df_txns,df_open],ignore_index=True,axis=0)
    #add loan closing to txns
    df_close=df_receipts[df_receipts["ST"]=="CL"]
    df_close=df_close[["GLNo","Name","Phone","RecDate","PrincRec","IntRec","TotalRec"]]
    df_close=df_close.rename(columns={"RecDate":"TxnDate","TotalRec":"TotalAmt","PrincRec":"Principal","IntRec":"Interest"})
    df_close["Type"]="CLS"
    df_txns = pd.concat([df_txns, df_close], ignore_index=True, axis=0)
    #add loan interim payments to txns
    df_int=df_receipts[df_receipts["ST"]=="INT"]
    df_int=df_int[["GLNo","Name","Phone","RecDate","PrincRec","IntRec","TotalRec"]]
    df_int=df_int.rename(columns={"RecDate":"TxnDate","TotalRec":"TotalAmt","PrincRec":"Principal","IntRec":"Interest"})
    df_int["Type"]="INT"
    df_txns = pd.concat([df_txns, df_int], ignore_index=True, axis=0)
    df_txns["Phone"]=df_txns["Phone"].fillna("NA")
    df_txns["InLoanInv"]=True
    df_txns.loc[df_txns["Phone"]=="NA","InLoanInv"]=False
    #create a transactions table
    #create a document object
    report=Document(name="Report_"+dataset+"_Customer.pdf",folder=report_folder)
    #define sections and pages
    #add pie chart of different type of transactions
    page1=report.add_page(n_sections=2)
    pie1_image_path=generate_pie_chart(df_txns,output_folder,"pie1","Type","TotalAmt")

    hist1_image_path=generate_gr_st_histogram(df_txns[["TotalAmt","InLoanInv","Type"]],output_folder,"hist1",group_col="InLoanInv",stack_col="Type",val_cols= ["TotalAmt"],ylabel="Amounts",unit_scale="Lakhs",agg=True)
    page1.add_section("image","Type of transactions",content=pie1_image_path,shsf=1.0)
    page1.add_section("image","Loan Inv vs Type transactions",content=hist1_image_path,shsf=1.0)
    # print and save doc
    report.render_doc()
    output_df=\
    {
        "LP01_No of unique GL":len(df_txns["GLNo"].unique()),
        "LP02_Total Principal Given":df_txns[df_txns["Type"]=="OPN"]["TotalAmt"].sum(),
        "LP08_Total Principal Returned Closed Loans":df_txns[df_txns["InLoanInv"]==False][df_txns["Type"]!="OPN"]["Principal"].sum(),
        "CB01_No of unique Cust Names":len(df_txns["Name"].unique()),
        "CB02_No of unique Phone Nos":len(df_txns["Phone"].unique()),
        "LP05_Average Loan Amount": df_txns[df_txns["Type"]=="OPN"]["TotalAmt"].mean(),
        "TXV01_Loans disbursed per day (15 week avg)":len(df_txns.loc[(df_txns["TxnDate"]>(rep_date-relativedelta(days=105)))][df_txns["Type"]=="OPN"]["GLNo"].unique())/90,
        "TXV02_Loans closed per day (15 week avg)": len(
            df_txns[(df_txns["TxnDate"] > (rep_date - relativedelta(days=105)))][df_txns["Type"] == "CLS"][
                "GLNo"].unique()) / 90,
        "TXV03_Loans int_paymnt per day (15 week avg)": len(
            df_txns[(df_txns["TxnDate"] > (rep_date - relativedelta(days=105)))][df_txns["Type"] == "INT"][
                "GLNo"].unique()) / 90,
        "LP09_No of closed loans":len(df_txns[df_txns["InLoanInv"]==False]["GLNo"].unique()),
    }
    return output_df


if __name__=="__main__":
    generate_customer_report("Oct23", rep_date=pd.to_datetime('2023-10-23'))
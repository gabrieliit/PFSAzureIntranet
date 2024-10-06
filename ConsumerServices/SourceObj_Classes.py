from DataServices.Extract.source_metadata import Sources
import os
import pandas as pd

class DataSource:
    @staticmethod
    def datasource_factory(input_folder,source_name,run_config,dataset=None):
        if source_name=="PendingLoan":
            source=PendingLoan(input_folder,source_name,run_config,dataset)
        elif source_name=="PendingLoan_30":
            source=PendingLoan_30(input_folder,source_name,run_config,dataset)
        elif source_name=="LoanStatus":
            source=LoanStatusReport(input_folder,source_name,run_config,dataset)
        elif source_name=="Receipt":
            source=ReceiptsTotal(input_folder,source_name,run_config,dataset)
        elif source_name=="Merged_Receipt_PendingLoan":
            source=Merged_Receipt_PendingLoan(input_folder, run_config,dataset)
        else:
            source=DataSource(input_folder,source_name,run_config,dataset)
        return source

    def __init__(self,input_folder, source_name,run_config,dataset=None):
        self.input_folder=input_folder
        source_file = Sources[source_name]["FileName"]
        try:
            format = Sources[source_name]["Format"]
        except KeyError:
            format = "xls"  # default is set to xl
        self.source_name=source_name
        self.input_format=format
        #check if dataset is provided, eg. this is required when the object needs to be created using historical trend dataset instead of reporting dataset
        if not dataset:#if no dataset is specified use the reporting date dataset in run_config. 
            dataset=run_config["Dataset"]
        self.filepath=os.path.join(input_folder,source_file+f"_{dataset}.{format}")
        self.run_config=run_config

    def load_data(self,skiprows=0, col_names=None):
        if self.input_format=="xls":
            self.df = pd.read_excel(self.filepath, skiprows=skiprows)
        elif self.input_format=="csv":
            self.df = pd.read_csv(self.filepath, skiprows=skiprows)
        if col_names:
            self.df.columns=col_names
        self.post_process()

    def post_process(self):
        pass#child classes to implement this method



class PendingLoan(DataSource):
    #implement post-processing
    def post_process(self):
        coll_rate=self.run_config["CollRate"]
        self.df["CollVal"] = coll_rate * self.df["Weight"]
        cols_to_drop = ["SlNo", "Notes", "Blank"]
        self.df = self.df.drop(columns=cols_to_drop)
        # Drop rows with null values
        self.df = self.df.dropna()
        # Convert the date column to datetime format
        self.df["LoanDate"] = pd.to_datetime(self.df["LoanDate"], format="mixed", dayfirst=True,errors="coerce")
        self.df["DueDate"] = pd.to_datetime(self.df["DueDate"], format="mixed", dayfirst=True,errors="coerce")
        self.df["NoticeDate"] = pd.to_datetime(self.df["NoticeDate"], format="mixed", dayfirst=True,errors="coerce")
        self.df["AgeDays"] = pd.to_timedelta(self.df["AgeDays"], unit="d")
        #Derived columns
        self.df["OverdueSince"]=self.run_config["RepDate"]-self.df["AgeDays"]
        # Assuming the first column is the date column and the rest are numeric columns
        summary_stat_df = self.df[["AmountGiven", "PrincBal", "CollVal"]]
        # Convert to numeric
        for col in summary_stat_df:
            pd.to_numeric(self.df[col], errors='coerce')

class PendingLoan_30(DataSource):
    #implement post-processing
    def post_process(self):
        cols_to_drop = ["Weight", "AvgBalperGm", "RecUpto", "Notes"]
        self.df = self.df.drop(columns=cols_to_drop)
        # Drop rows with null values
        self.df = self.df.dropna()
        # Convert the date column to datetime format
        date_cols = ["LoanDate", "DueDate"]
        for col in date_cols:
            self.df[col] = pd.to_datetime(self.df[col], format="mixed", dayfirst=True)
        sum_tot_bal = self.df["TotalBal"].sum()
        self.df["wtdPendingDays"] = self.df["PendingDays"] * self.df["TotalBal"] / sum_tot_bal
        self.df["wtdPendingDays"] *= self.df["wtdPendingDays"].count()  # make comparable to unweighted individual entries
        self.df["RemTenure"] = self.df["DueDate"] - self.run_config["RepDate"]
        self.df["RemTenure"] = self.df["RemTenure"].apply(lambda x: x.days)
        # Assuming the first column is the date column and the rest are numeric columns
        summary_cols = ["AmountGiven", "IntRate", "PrincBal", "TotalBal", "CollVal", "PendingDays", "wtdPendingDays"]
        summary_stat_df = self.df[summary_cols]
        # Convert to numeric
        for col in summary_stat_df:
            pd.to_numeric(self.df[col], errors='coerce')

class LoanStatusReport(DataSource):
    #implement post-processing
    def post_process(self):
        self.df.columns = ["LoanDate", "GLNo", "Name", "AmountGiven", "IntBal", "PrincBal"]
        # Drop rows with null values
        self.df = self.df.dropna()
        # Convert the date column to datetime format
        date_column = self.df.columns[0]
        self.df[date_column] = pd.to_datetime(self.df[date_column], errors='coerce')

class ReceiptsTotal(DataSource):
    #implement post-processing
    def post_process(self):
        cols_to_drop = ["Weight"]
        self.df = self.df.drop(columns=cols_to_drop)
        self.df["ST"]=self.df["ST"].fillna("INT")
        # Drop rows with null values
        self.df = self.df.dropna()
        # Convert the date column to datetime format
        date_cols=["RecDate"]
        self.df[date_cols]=self.df[date_cols].apply(pd.to_datetime)

class CompoundDataSource():
    def __init__(self,input_folder,merge_params,run_config,dataset=None):#merge two datasources
        """
        :param input_folder:
        :param merge_params:
        {
            "MergeEntities":
            {
                "Left":
                {
                    "SourceName":
                    "Cols":
                    "MergeCol":
                },
                "Right":
                {
                    "SourceName":
                    "Cols":
                    "MergeCol":
                },
            },
            "How":
        }
        :param run_config:
        """
        for source, params in merge_params["MergeEntities"].items():
            params["SourceObj"] = DataSource.datasource_factory(input_folder, params["SourceName"], run_config, dataset)
            self.merge_params = merge_params
    def load_data(self):
        for source, params in self.merge_params["MergeEntities"].items():
            params["SourceObj"].load_data(skiprows=Sources[params["SourceName"]]["SkipRows"], col_names=Sources[params["SourceName"]]["ColNames"])
            if params["Cols"]!="all":
                params["SourceObj"].df_merge=params["SourceObj"].df[params["Cols"]]
            else:
                params["SourceObj"].df_merge = params["SourceObj"].df
        self.df = pd.merge(self.merge_params["MergeEntities"]["Left"]["SourceObj"].df_merge, self.merge_params["MergeEntities"]["Right"]["SourceObj"].df_merge,
                           left_on=self.merge_params["MergeEntities"]["Left"]["MergeCol"], right_on=self.merge_params["MergeEntities"]["Right"]["MergeCol"],
                           how=self.merge_params["How"])


class Merged_Receipt_PendingLoan(CompoundDataSource):
    def __init__(self,input_folder,run_config,dataset=None):
        self.input_folder=input_folder
        self.run_config=run_config
        merge_params=\
        {
            "MergeEntities":
            {
                "Left":
                {
                    "SourceName": "Receipt",
                    "Cols": "all",
                    "MergeCol": "GLNo"
                },
                "Right":
                {
                    "SourceName": "PendingLoan",
                    "Cols": ["GLNo","Phone"],
                    "MergeCol": "GLNo",
                },
            },
            "How": "left"
        }
        super().__init__(input_folder,merge_params,run_config,dataset)
    def load_data(self):
        #implement post-processing
        super().load_data()
        df_loans=self.merge_params["MergeEntities"]["Right"]["SourceObj"].df
        df_receipts = self.df
        # add loan opening to txns
        df_open = df_loans[["GLNo", "Name", "Phone", "LoanDate", "AmountGiven"]]
        df_open = df_open.rename(columns={"LoanDate": "Date", "AmountGiven": "Principal"})
        df_open["Type"] = "OPN"
        df_open["Interest"] = 0.0
        df_open["TotalAmt"] = df_open["Principal"]
        # df_txns=df_txns.rename({"LoanDate":"Date","AmountGiven":"Amount"})
        df_txns = pd.DataFrame(columns=["GLNo", "Name", "Phone", "Date", "Principal", "Interest", "TotalAmt", "Type"])
        df_txns = pd.concat([df_txns, df_open], ignore_index=True, axis=0)
        # add loan closing to txns
        df_close = df_receipts[df_receipts["ST"] == "CL"]
        df_close = df_close[["GLNo", "Name", "Phone", "RecDate", "PrincRec", "IntRec", "TotalRec"]]
        df_close = df_close.rename(
            columns={"RecDate": "Date", "TotalRec": "TotalAmt", "PrincRec": "Principal", "IntRec": "Interest"})
        df_close["Type"] = "CLS"
        df_txns = pd.concat([df_txns, df_close], ignore_index=True, axis=0)
        # add loan interim payments to txns
        df_int = df_receipts[df_receipts["ST"] == "INT"]
        df_int = df_int[["GLNo", "Name", "Phone", "RecDate", "PrincRec", "IntRec", "TotalRec"]]
        df_int = df_int.rename(
            columns={"RecDate": "Date", "TotalRec": "TotalAmt", "PrincRec": "Principal", "IntRec": "Interest"})
        df_int["Type"] = "INT"
        df_txns = pd.concat([df_txns, df_int], ignore_index=True, axis=0)
        df_txns["Phone"] = df_txns["Phone"].fillna("NA")
        df_txns["InLoanInv"] = True
        df_txns.loc[df_txns["Phone"] == "NA", "InLoanInv"] = False
        self.df=df_txns


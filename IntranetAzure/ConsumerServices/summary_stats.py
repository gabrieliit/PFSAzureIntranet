# summary_stats.py
import pandas
import pandas as pd
import matplotlib.pyplot as plt


import os

def generate_6m_rolling_summary_stats(df, min_date, max_date, output_path=None,drop_stats=[]):
    # Convert input strings to datetime objects
    min_date = pd.to_datetime(min_date)
    max_date = pd.to_datetime(max_date)

    # Generate 6-month intervals
    intervals = pd.date_range(start=min_date, end=max_date, freq='6ME')

    # Convert intervals to a list of tuples representing start and end dates of each interval
    interval_tuples = [(intervals[i], intervals[i+1]) for i in range(len(intervals)-1)]

    # Define a function to map dates to intervals
    def map_to_interval(date):
        for i, (start, end) in enumerate(interval_tuples):
            if start <= date < end:
                return i

    # Add a new column to the DataFrame with the mapped intervals
    df['Interval'] = df.iloc[:, 0].apply(map_to_interval)

    # Create subsets of the DataFrame based on interval values
    subset_dfs = [df[df['Interval'] == i] for i in range(len(interval_tuples))]
    # Calculate summary statistics for each subset
    subset_stats=[]
    for subset in subset_dfs:
        subset.drop("Interval",axis=1,inplace=True)
        stats=subset.describe()
        stats=stats.drop(index="count")
        for stat in drop_stats:
            stats=stats.drop(index=stat)
        subset_stats.append(stats)
    stats=list(subset_stats[0].index)
    columns=subset_stats[0].columns
    # Drop non-numeric columns from the list of amount columns
    numeric_cols = [col for col in columns if pd.to_numeric(subset_stats[0][col], errors='coerce').notnull().all()]
    stat_lineplot_data={}
    interval_mid_dates = [(start + (end - start) / 2) for start, end in interval_tuples]
    for col in numeric_cols:
        df = pd.DataFrame()
        i=0
        for subset in subset_stats:
            xval=interval_mid_dates[i]
            df[xval]=subset[col]
            i+=1
        stat_lineplot_data[col]=df

    # Create plots for each numeric col with line plots for each statistic
    lineplots=[]
    output_df = pandas.DataFrame(columns = ["col_name","stat"] + df.columns.to_list())
    for col in stat_lineplot_data:
        df=stat_lineplot_data[col]
        plt.figure(figsize=(10, 6))
        for stat in stats:
            plt.plot(interval_mid_dates, df.loc[[stat]].values.flatten().tolist(), label=stat)
        # Set labels and title
        plt.xlabel('Interval Mid-Date')
        plt.ylabel('Value')
        plt.title(f'Summary Stats {col} Over 6-Month Intervals')
        plt.legend()
        # Save the plot as an image file if output_path is provided
        if output_path:
            if os.path.exists(output_path + f'/{col}.png'):
                os.remove(output_path + f'/{col}.png')
            else:#if file doesnt exist check if output folder exists and create it
                if not os.path.exists(output_path):
                    os.makedirs(output_path)
            plt.savefig(output_path + f'/{col}.png', bbox_inches='tight')
            lineplots.append(output_path + f'/{col}.png')
        df["col_name"]=col
        df["stat"]=stats
        output_df=pd.concat([output_df,df], ignore_index=True)
    return lineplots,list(stat_lineplot_data.keys()),output_df

def partition_summary_stats(df,val_cols,partition_col,calc_incr=False,stats_to_drop=[]):
    #get unique values of partiitoning columns
    partition_vals = df[partition_col].unique()
    df=df[[partition_col]+val_cols]
    output_df=pd.DataFrame(columns=[partition_col]+val_cols)
    for val in partition_vals:
        #subset df by partiition
        partition=df.loc[df[partition_col]==val][val_cols]
        summary_stats=partition.describe()
        summary_stats[partition_col]=val
        summary_stats["Stat"]=summary_stats.index
        if calc_incr:
            summary_stats[val_cols]=summary_stats[val_cols].diff().fillna(summary_stats[val_cols])
        summary_stats.drop(stats_to_drop,inplace=True)
        output_df=pd.concat([output_df,summary_stats],ignore_index=True)
    return output_df


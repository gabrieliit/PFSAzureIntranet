# histogram_plot.py
import pandas as pd
import matplotlib.pyplot as plt
import os
from matplotlib.ticker import FuncFormatter as ff
import seaborn as sns
import numpy as np

def generate_pie_chart(df,output_folder,filename,group_col,val_col,unit_scale=100000,bins=None):
    #Create Bins by partitioning group COl
    if bins:
        #create bin labels
        bin_labels=[None for i in range(len(bins))]
        bin_labels[0]=f"<{str(bins[0])}"
        bin_labels[1:]=[f"{bins[i]}-{bins[i+1]}" for i in range(len(bins)-1)]
        bin_labels.append(f">{bins[len(bins)-1]}")
        #make the bins range from -inf to +inf
        bins.append(float("inf"))
        bins.insert(0,float("-inf"))
        df["bins"] = pd.cut(df[group_col], bins, labels=bin_labels)
        group_col="bins"
    #df should have two cols group_col, val_col
    summary_df=df.groupby(group_col).agg({val_col: ["sum", "count"]})
    summary_df.columns = ['{}_{}'.format(col[0], col[1]) for col in summary_df.columns]
    plt.figure()
    plt.subplot(1,2,1)
    plt.pie(summary_df[f"{val_col}_sum"], labels=(summary_df[f"{val_col}_sum"]/unit_scale).apply(lambda x:'{:.2f}'.format(x)),textprops={'fontsize':6})
    plt.axis("equal")
    scales={100000:"Lakhs"}
    try:
        scale=f"({scales[unit_scale]})"
    except KeyError:
        scale=""
    plt.title(f"Sum Distribution {scale}")
    plt.subplot(1,2,2)
    wedges,texts=plt.pie(summary_df[f"{val_col}_count"], labels=summary_df[f"{val_col}_count"])
    plt.axis("equal")
    plt.title("Count Distribution")
    plt.legend(wedges,summary_df.index,loc="lower center",fontsize=6)
    # Save the histogram plot as an image
    # Ensure that the directory exists, or create it if it doesn't
    os.makedirs(output_folder, exist_ok=True)
    pie_image_path = os.path.join(output_folder, filename)
    plt.savefig(pie_image_path, format='png')
    plt.close()
    return pie_image_path




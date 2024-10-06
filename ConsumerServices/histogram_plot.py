# histogram_plot.py
import pandas as pd
import matplotlib.pyplot as plt
import os
from matplotlib.ticker import FuncFormatter as ff
import seaborn as sns
import numpy as np

scale_labels = \
    {
        "Lakhs":100000
    }
def generate_gr_st_histogram(df,output_folder,filename,group_col,stack_col,val_cols,bar_width='normal',ylabel="",unit_scale=None, agg=False):
    #generate grouped and stacked histogram
    if agg:
        df_agg=df.groupby([group_col,stack_col])[val_cols].agg("sum")
        rows=df_agg.index.tolist()
        df_new=pd.DataFrame(rows,columns=[group_col,stack_col])
        df_agg=df_agg.reset_index()
        for col in val_cols:
            df_new[col]=df_agg[col]
        df=df_new

    groups=df[group_col].unique()
    stacks=df[stack_col].unique()
    widths={"normal":0.35}
    width=widths[bar_width]
    group_spacing=width*len(val_cols)+2*width
    group_pos=np.arange(stop=len(groups)*group_spacing,step=group_spacing)
    palettes=["green","blue","gold","pink","grey"]
    val_no=0
    plt.figure()
    fig,ax=plt.subplots()
    for col in val_cols:
        bar_pos=group_pos+val_no*width
        bottom=np.zeros(len(groups))
        stack_no=0
        for stack in stacks:
            data=np.array(df[df[stack_col]==stack][col])
            ax.bar(bar_pos,data,width*0.9,
                    bottom=bottom,
                    color=sns.light_palette(palettes[val_no%len(palettes)],10)[stack_no%10],
                    lw=0.5,
                    edgecolor='white',
                    label=col+"_"+stack)
            bottom+=data
            stack_no+=1
        val_no+=1
    plt.title(f'Distributions by {group_col}',fontsize=8)
    ylabel = f"{ylabel} ({unit_scale})" if unit_scale else str(ylabel)
    plt.ylabel( ylabel, color='green',fontsize=6)
    #plt.legend(title=df.columns[0], bbox_to_anchor=(1.05, 1), loc='upper left',fontsize=6)
    plt.legend(fontsize=6,loc="best")
    plt.tight_layout()
    plt.xticks(group_pos, groups, rotation=45,fontsize=6)
    if unit_scale:
        ax.yaxis.set_major_formatter(ff(lambda x, pos: f"{x / scale_labels[unit_scale]:.2f}"))
    # Save the histogram plot as an image
    hist_image_path = os.path.join(output_folder, filename)
    plt.savefig(hist_image_path, format='png')
    plt.close()
    return hist_image_path
def generate_histograms(df, output_folder,filename,count_columns,val_cols,create_bin_type="None",bar_width="normal",y_scale="Lakhs",**kwargs):
    # Extract month and year from the date column
    try:
        xlabel=kwargs["xlabel"]
    except KeyError:
        xlabel="Month" if create_bin_type=="Months" else ""
    if create_bin_type=="Months":
        # Assuming the first column is the date column and the rest are amount-related columns
        date_column = df.columns[0]
        df[date_column]=pd.to_datetime(df[date_column])
        df['Month'] = df[date_column].dt.month
        df['Year'] = df[date_column].dt.year
        # Create a bins column that concatenates year and month in 'YYYY_MM' format
        df['bins'] = df['Year'].astype(str) + '_' + df['Month'].astype(str).str.zfill(2)

    elif create_bin_type=="Equal_partitions":
        try:
            nbins=kwargs['nbins']
            df['bins'] = pd.cut(df[df.columns[0]], bins=nbins, precision=2).apply(lambda x:x.mid)
        except KeyError as e:
            print (f"Error {e} occured. When using Equal_partitions as create_bin_type, please provide an nbins param tot he generate_histogram function")
    else:
        df['bins']=df.columns[0] #assume first column contains bins
    #set width param
    widths=\
        {
            "normal":0.8,
            "high":1.6,
            "vh":16
        }

    width=widths[bar_width]

    # Drop non-numeric columns from the list of amount columns
    amount_columns_numeric = [col for col in val_cols if pd.to_numeric(df[col], errors='coerce').notnull().all()]

    # Select only numeric columns along with 'YearMonth'
    df_numeric = df[amount_columns_numeric + ['bins']]

    # Plot each amount column separately for each year-month combination
    plt.figure()
    fig, ax1 = plt.subplots()
    ax2=ax1.twinx()
    for amount_column in val_cols:
        ax1.plot(df_numeric.groupby(['bins'])[amount_column].sum(), label=amount_column)
    try:
        unit_scale= scale_labels[y_scale]
    except KeyError:
        unit_scale=""
    ax1.set_ylabel(f'Amounts ({y_scale})', color='blue',fontsize=6)
    ax1.set_xlabel(f'{xlabel}', color='red', fontsize=6)
    ax1.yaxis.set_major_formatter(ff(lambda x,pos:f"{x/scale_labels[y_scale]:.2f}"))
    #plot count histograms
    for column in count_columns:  # For each column on which we are grouping unique counts
        df_counts = df.groupby('bins')[column].size()
        ax2.bar(df_counts.index, df_counts.values, color='green', label=column,width=width)
    plt.title(f'Amount and Count Distributions by {df.columns[0]}',fontsize=12)
    ax2.set_ylabel('Counts', color='green',fontsize=6)
    #plt.legend(title=df.columns[0], bbox_to_anchor=(1.05, 1), loc='upper left',fontsize=6)
    ax1.tick_params(labelsize=6,labelrotation=90)
    ax1.legend(fontsize=6,loc="upper left")
    ax2.tick_params(labelsize=6, labelrotation=90)
    ax2.legend(fontsize=6,loc='upper right')
    plt.tight_layout()

    # Save the histogram plot as an image
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    hist_image_path = os.path.join(output_folder, filename)
    plt.savefig(hist_image_path, format='png')
    plt.close()

    return hist_image_path

"""
    xticks=df['bins'].unique()
    xticks_slice=xticks[::4].tolist()
    xticks_pos=list(range(0, len(xticks), 4))
    plt.xticks(xticks_pos,xticks_slice,rotation=90,ha='right')
"""

# Additional functions or content as needed

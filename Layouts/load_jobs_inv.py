import dash
import dash_core_components as dcc
import dash_html_components as html
from dash import Output, Input,State,callback_context
import dash_bootstrap_components as dbc
import dash_table
import pandas as pd
from bson import ObjectId
import io
from datetime import date
#import project modules
from Layouts import styles, shared_components as sc
from ProducerServices.Extract.source_metadata import ProducerSources
from ConsumerServices.BuildDataset import create_dataset

def draw_page_content():
    layout = html.Div(
        [
            html.H3("Load Jobs List"),
            dbc.Row(
            [
                dbc.Col(dbc.Button("Refresh Job Details",id="dataloadjobs-btn-refresh-jobs"),width=3),
                dbc.Col(dbc.RadioItems(options=[
                    {"label":"Last Run","value":1},
                    {"label":"Last 5 Runs","value":2},
                    {"label":"All Runs","value":3},
                ], 
                value=1,
                inline=True,
                id="dataloadjobs-rdo-jobsfilter"),width=6),
                dbc.Col(dbc.Button("View Write Errors",id="dataloadjobs-btn-view-errors",disabled=True),width=3),
            ]
            ),
            html.Div(id="dataloadjobs-div-job-details"),
            html.Div([
                html.H3("Write Errors List"),
                html.Div(dbc.Alert("Refresh Job details, select a job from jobs list and click View Write Errors button",color="warning"),id="dataloadjobs-div-error-details"),
            ]
            )
        ],
        style=styles.CONTENT_STYLE
    )
    return layout

def register_callbacks(app):
    @app.callback(
        Output("dataloadjobs-div-job-details","children"),
        Output("dataloadjobs-btn-view-errors","disabled"),
        Input("dataloadjobs-btn-refresh-jobs",'n_clicks'),
        State("dataloadjobs-rdo-jobsfilter",'value')
    )
    def refresh_job_details(nclicks,job_filter):
        if nclicks:
            dsb=create_dataset.DatasetBuilder("LoadJobs")
            dsb.create_dataset(dtype="str")
            start_times=dsb.output['JobStart'].drop_duplicates().sort_values(ascending=False,ignore_index=True)
            start_time_filter=pd.to_datetime(pd.Series({'year':1900,'month':1,'date':1}))#include all jobs
            if job_filter==1:
                start_time_filter=start_times[0]#include latest jobs
            elif job_filter==2:
                start_time_filter=start_times[min(4,len(start_times)-1)]#include last 5 jobs
            result=sc.df_to_dash_table(dsb.output[dsb.output['JobStart']>=start_time_filter],id_str="dataoadjobs-tbl-job-details")
            disable= False if (len(dsb.output)>0) else True
        else:
            result=dash.no_update
            disable=True
        return result,disable
    
    @app.callback(
        Output("dataloadjobs-div-error-details","children"),
        Input("dataloadjobs-btn-view-errors","n_clicks"),
        State("dataloadjobs-div-job-details","children"),
    )
    def view_write_errors(nclicks,jobs_tbl):
        if nclicks:
            try:
                active_cell=jobs_tbl['props']['active_cell']
                row=active_cell['row']
                job_id=ObjectId(jobs_tbl['props']['data'][row]['JobID'])
                dsb=create_dataset.DatasetBuilder("WriteErrors")
                dsb.create_dataset(filters={"_id":job_id})
                result=sc.df_to_dash_table(dsb.output)
                enable= True if (len(dsb.output)>0) else False
            except KeyError:
                result=dbc.Alert("Select a row from the Job Details table and then click the view errors button", color="warning")
        else:
            result=dash.no_update
        return result
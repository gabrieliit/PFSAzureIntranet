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
from Pages import styles, shared_components as sc
from ProducerServices.Extract.source_metadata import ProducerSources
from ConsumerServices.DatasetTools import create_dataset

def draw_page_content():
    layout = html.Div(
        [
            html.H3("Load Jobs List"),
            dbc.Row(
            [
                dbc.Col(dbc.Button("Refresh Job Details",id="dataloadjobs-btn-refresh-jobs"),width=2),
                dbc.Col(dbc.RadioItems(options=[
                    {"label":"Last Run","value":1},
                    {"label":"Last 5 Runs","value":2},
                    {"label":"All Runs","value":3},
                ], 
                value=1,
                inline=True,
                id="dataloadjobs-rdo-jobsfilter"),width=4),
                dbc.Col(dbc.Button("View Write Errors",id="dataloadjobs-btn-view-errors",disabled=True),width=2),
                dbc.Col(dbc.Button("View CheckSums",id="dataloadjobs-btn-view-checksums",disabled=True),width=2),            
            ]
            ),
            html.Div(id="dataloadjobs-div-job-details"),
            html.Div([
                html.H3(id="dataloadjobs-H3-errors-and-checksums"),
                html.Div(dbc.Alert("Refresh Job details, select a job from jobs list and click View Write Errors or View CheckSums button",color="warning"),id="dataloadjobs-div-error-details"),
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
        Output("dataloadjobs-btn-view-checksums","disabled"),
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
        return result,disable,disable
    
    @app.callback(
        Output("dataloadjobs-div-error-details","children"),
        Output("dataloadjobs-H3-errors-and-checksums","children"),
        Input("dataloadjobs-btn-view-errors","n_clicks"),
        Input("dataloadjobs-btn-view-checksums","n_clicks"),
        State("dataloadjobs-div-job-details","children"),
    )
    def view_errors_and_checksums(nclicks,nclicks_cs,jobs_tbl):
        result=dash.no_update#no UI updated if no buttons have been clicked
        ctx=dash.callback_context
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
        heading=""
        if trigger_id in ["dataloadjobs-btn-view-errors","dataloadjobs-btn-view-checksums"]:
            try:
                active_cell=jobs_tbl['props']['active_cell']
                row=active_cell['row']
                job_id=ObjectId(jobs_tbl['props']['data'][row]['JobID'])
                if trigger_id=="dataloadjobs-btn-view-errors"  :#display write errors
                    dsb=create_dataset.DatasetBuilder("WriteErrors")
                    heading="Write Errors List"
                elif trigger_id=="dataloadjobs-btn-view-checksums":#display checksums
                    dsb=create_dataset.DatasetBuilder("CheckSums")
                    heading="Checksums"
                dsb.create_dataset(filters={"_id":job_id})
                result=sc.df_to_dash_table(dsb.output)   
            except KeyError:
                result=dbc.Alert("Select a row from the Job Details table and then click the view errors button", color="warning")
        return result,heading
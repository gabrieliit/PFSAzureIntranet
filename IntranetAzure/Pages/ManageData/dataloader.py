import dash
import dash_core_components as dcc
import dash_html_components as html
from dash import Output, Input,State,callback_context
import dash_bootstrap_components as dbc
# Import dash_table with compatibility fallback
try:
    import dash_table
except ImportError:
    try:
        from dash import dash_table
    except ImportError:
        # Import compatibility module
        from dash_table_compat import dash_table
import pandas as pd
import base64
import io
from datetime import date
#import project modules
from Pages import styles, shared_components as sc
from ProducerServices.Extract.source_metadata import ProducerSources
from CommonDataServices import data_extractor as de
from ProducerServices.Transform import data_transformer as dt

def draw_page_content():
    layout = html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(dbc.Label("Filetype"),width=3),
                    dbc.Col(
                        dbc.Select(
                            id='dataloader-sel-filetype',
                            options=list(ProducerSources.keys()),
                            placeholder='select the type of input file you are uploading',
                            value="Receipt"
                        ),
                        width=9
                    )
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(dbc.Label("COB Date"),width=3),
                    dbc.Col(
                        dcc.DatePickerSingle(
                            id='dataloader-dp-sel-cob-date',
                            display_format='DD-MM-YYYY',
                            style={"width":"200px"},
                            date=date.today()
                        ),
                        width=3
                    ),
                    dbc.Col(
                        dbc.Label("Select from calendar or enter manually using DD-MM-YYYY format",color="secondary"),
                        width=6
                    )
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Upload(
                            id='dataloader-upl-sel-file',
                            children=dbc.Button('Select and View File',id='dataloader-upl-btn-sel-file',disabled=True),
                            multiple=False,
                            disabled=True),
                            width=2
                    ),
                    dbc.Col(
                        dbc.Button("Upload Data", id='dataloader-btn-upload-to-mdb',disabled=True),
                        width=2
                    ),
                    dbc.Col([dbc.Input(id="dataloader-ip-rowlimit",placeholder="Enter max source rows to load. Leave blank to load all.")],
                            width=4)
                ]

            ),
            dcc.ConfirmDialog(id='dataloader-cdg-upload-to-mdb'),
            dcc.Store(id='dataloader-store-file-data'),
            html.Div(id='dataloader-div-view-file'),
            dcc.Markdown(id='dataloader-md-upload-mdb-msg',),
        ],
        style=styles.CONTENT_STYLE
        )
    return layout
    
def register_callbacks(app):
    @app.callback(
        Output('dataloader-upl-btn-sel-file','disabled'),
        Output('dataloader-upl-sel-file','disabled'),
        Input('dataloader-sel-filetype','value'),
        Input('dataloader-dp-sel-cob-date','date')
    )
    def enable_file_selection(filetype,cobdate):
        if filetype and cobdate:
            return False,False  
        else: 
            return True,True
    
    @app.callback(
        Output('dataloader-div-view-file', 'children'),
        Output('dataloader-btn-upload-to-mdb','disabled'),
        Output('dataloader-store-file-data','data'),
        [Input('dataloader-upl-sel-file', 'contents')],
        [
            State('dataloader-upl-sel-file', 'filename'),
            State('dataloader-sel-filetype','value'),
            State('dataloader-dp-sel-cob-date','date'),
        ]
    )
    def extract_data(contents,filename,filetype,date_value):
        children=dash.no_update
        disabled=dash.no_update
        data=dash.no_update
        if date_value is not None:
            date_object = date.fromisoformat(date_value)
            date_string = date_object.strftime('%d%m%Y')
            format_filename=filetype+"_"+date_string+"."+ProducerSources[filetype]['Format']
            if filename != format_filename:
                #the selected filename is not in expected format
                children=dbc.Alert(f"Filename should be {format_filename}", color="warning")
                disabled=dash.no_update
                data=dash.no_update
            elif contents is not None:
                content_type, content_string = contents.split(',')
                decoded = base64.b64decode(content_string)
                dtypes=ProducerSources[filetype]["ColTypes"]
                df = pd.read_excel(io.BytesIO(decoded),skiprows=ProducerSources[filetype]["SkipRows"]-1)
                df.columns=dtypes.keys()
                for col,dtype in dtypes.items():#retain string types for dsiplay purposes  
                    if type(dtype)==str:
                        if dtype.startswith("date"):
                            str_format=dtype[dtype.find("date-")+len("date-"):]#date dtype is specified as date-formatstring in ColTypes metadate
                            # Convert the date strings to dd-mm-yyyy format
                            df[col] = pd.to_datetime(df[col],format=str_format,errors="coerce").dt.strftime(str_format)  
                #create dash table to display
                children=sc.df_to_dash_table(df,'dataloader-tbl-view-data')
                disabled=False
                data=df.to_dict("records")
        return children,disabled,data
    
    @app.callback(
        Output('dataloader-md-upload-mdb-msg','children'),
        Output('dataloader-cdg-upload-to-mdb',"message"),
        Output('dataloader-cdg-upload-to-mdb',"displayed"),
        State('dataloader-store-file-data','data'),
        State('dataloader-sel-filetype','value'),
        State('dataloader-dp-sel-cob-date','date'),
        Input('dataloader-btn-upload-to-mdb','n_clicks'),
        Input('dataloader-cdg-upload-to-mdb','submit_n_clicks'),
        State("dataloader-ip-rowlimit", "value"),
    )
    def transform_load_data(data,source,cob,n_clicks,submit_n_clicks,row_limit):
        ctx = dash.callback_context
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
        if trigger_id=='dataloader-btn-upload-to-mdb':
            msg="This will trigger a job to write records to database and may take several minutes to complete.\nYou can view progress of the write job in LoadJobsInventory tab under MI menu.\nClick OK to proceed or Cancel to abort"
            return dash.no_update, msg,True
        elif trigger_id=='dataloader-cdg-upload-to-mdb' and submit_n_clicks:
            raw_df=pd.DataFrame(data)
            source_obj=de.DataSource_Old(source,raw_df)
            df=source_obj.df
            dtypes=ProducerSources[source]["ColTypes"].copy()
            #add derived col dtypes
            try:
                der_cols=ProducerSources[source]["DerivedCols"]["TargetCols"]
                der_types=ProducerSources[source]["DerivedCols"]["TargetDataTypes"]
                for col,dtype in zip(der_cols,der_types):
                    dtypes[col]=dtype
            except: #no derived cols for data source
                pass
            #explicity cast data types to match mongo db schema
            for col,dtype in dtypes.items():
                if type(dtype)==str:#currently only date type objects are specified as strings in the ColTypes metadata
                    if dtype.startswith("date"):
                        str_format=dtype[dtype.find("date-")+len("date-"):]#date dtype is specified as date-formatstring in ColTypes metadate
                        df[col]=pd.to_datetime(df[col],errors='coerce',format=str_format,)
                        df[col]=df[col].fillna(pd.to_datetime('1900-01-01'))
                else:
                    df[col]=df[col].astype(dtype)
            #update source obj.df
            source_obj.df=df
            global mapper#define as global so we can access it in other callbacks
            mapper=dt.DataTransformer(source_obj,cob)
            max_rows=len(df) if (row_limit==None or row_limit=='') else int(row_limit) 
            job_id_list=mapper.transform_data(row_limit=max_rows)
            mapper.load_data(batch_size=50)
            njobs=len(job_id_list)
            msg=f"{njobs} collection(s) updated. You can track jobs in MI->Data Load Jobs tab using the following IDs - \n {str(job_id_list)} "
            return msg, dash.no_update,dash.no_update
        return dash.no_update,dash.no_update,dash.no_update
    

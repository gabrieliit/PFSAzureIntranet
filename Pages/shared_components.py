from dash import dcc, html,dash_table
import dash_bootstrap_components as dbc
#import project modules
from Pages import styles

# Define your company logo and name (customize as needed)
company_logo_url = "https://example.com/logo.png"
company_name = "Acme Company"
external_stylesheets=[dbc.themes.BOOTSTRAP]

#utility function to convert df to a dash table
def df_to_dash_table(df,id_str="default"):
    df=df.astype(str)
    return dash_table.DataTable(
                id=id_str,
                columns=[{"name": col, "id": col} for col in df.columns],
                data=df.to_dict('records'),
                # Other DataTable properties (e.g., pagination, sorting) can be customized here
                style_table=
                {
                    'overflowY': 'scroll',  # Enable vertical scrolling
                    'overflowX': 'scroll',
                    'maxHeight': '300px',  # Set the maximum height
                    'width': '100%',
                    'table-layout': 'fixed'
                },
                style_cell={
                    "textAlign": "left",
                    'minWidth': '150px',
                },
                fixed_rows={"headers": True},  # Freeze the header row
            )

# Top banner
def draw_top_banner(login=False,user_name=""):
    if login:
        link_label="Logout"
        href="/logout"
        user_label=f"{user_name} is logged in"
    else:
        link_label="Login"
        href="/login"
        user_label=f""
    top_banner=[
            html.H1(company_name, style={"display": "inline-block", "margin-left": "10px"}),
            html.Div(f"{user_label}",id="topBanner_lbl_user", style={"float": "right", "margin-right": "20px"}),
            dcc.Link(f"{link_label}",id="topBanner_link_login", href=f"{href}", style={"float": "right", "margin-right": "10px"}),
        ]
    return top_banner

    # Sidebar menu (customize menu items)
side_bar = html.Div(
    [
        html.Img(src=company_logo_url, alt="Company Logo", style={"height": "50px"}),
        html.Hr(),
        html.P("Select links to navigate", className="lead"),
        dbc.Nav(
            [
                dbc.NavLink("Home",href="/", active="exact"),
                dbc.NavLink("Accounts", href="/accounts", active=False),
                dbc.NavLink("Customers", href="/customers", active=False),
                dbc.NavLink("Transactions", href="/transactions", active=False),
                dbc.DropdownMenu
                (
                    [
                        dbc.DropdownMenuItem("Data Loader", href="/dataloader", active=False),
                        dbc.DropdownMenuItem("Data Load Jobs", href="/dataloadjobs", active=False),
                    ],
                    label="Manage Data", 
                    nav=True,
                ),
                dbc.NavLink("Admin", href="/admin", active="exact"),
                dbc.NavLink("Scratch", href="/scratch", active="exact"),
            ],
            vertical=True,
        ),
    ],
    style=styles.SIDEBAR_STYLE,
    id="outline_side_bar"
)

def toggle_modal(n, is_open):
    #if button is clicked open the corresponding modal
    if n:
        return not is_open
    return is_open
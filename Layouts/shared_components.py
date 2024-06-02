from dash import dcc, html
import dash_bootstrap_components as dbc
#import project modules
from Layouts import styles

# Define your company logo and name (customize as needed)
company_logo_url = "https://example.com/logo.png"
company_name = "Acme Company"
external_stylesheets=[dbc.themes.BOOTSTRAP]

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
            dbc.NavLink("Home", href="/", active="exact"),
            dbc.NavLink("MI", href="/mi", active="exact"),
            dbc.NavLink("Loans", href="/loans", active="exact"),
            dbc.NavLink("Receipts", href="/receipts", active="exact"),
            dbc.NavLink("Accounts", href="/accounts", active="exact"),
            dbc.NavLink("Customers", href="/customers", active="exact"),
            dbc.NavLink("Admin", href="/admin", active="exact"),
            dbc.NavLink("Scratch", href="/scratch", active="exact"),
        ],
        vertical=True,
        pills=True,
        ),
    ],
    style=styles.SIDEBAR_STYLE,
    id="outline_side_bar"
)

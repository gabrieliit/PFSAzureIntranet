import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import msal
from flask import redirect, request, session
from Layouts import homepage
"""
setx MSFT_AUTH_CLIENT_ID "fed278c4-f614-4379-b8b8-68c592f3e769"
setx MSFT_AUTH_CLIENT_SECRET "Oth8Q~zeYloemXGrm6PSAQd2KEt129Qjlz2b-bIf"
setx MSFT_AUTH_REDIRECT_URI "http://localhost:8050/.auth/login/aad/callback"
setx MSFT_AUTH_TENANT_ID "f60dfc78-fca1-4c4c-9112-b231e45712f9"

echo $MSFT_AUTH_CLIENT_ID
echo $MSFT_AUTH_CLIENT_SECRET
echo $MSFT_AUTH_REDIRECT_URI
echo $MSFT_AUTH_TENANT_ID
"""
# Load environment variables
CLIENT_ID = os.getenv('MSFT_AUTH_CLIENT_ID')
CLIENT_SECRET = os.getenv('MSFT_AUTH_CLIENT_SECRET')
TENANT_ID=os.getenv('MSFT_AUTH_TENANT_ID')
AUTHORITY = F'https://login.microsoftonline.com/{TENANT_ID}'
REDIRECT_URI = os.getenv('MSFT_AUTH_REDIRECT_URI')
SCOPE = ["User.Read"]

page_layout = homepage.draw_homepage(homepage_ext=[dcc.Link("login",href="/login")])

#ALLOWED_USERS = os.getenv('ALLOWED_USERS', '').split(';')
def register_callbacks(app):
    if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
        raise ValueError("Please set the CLIENT_ID, CLIENT_SECRET, and REDIRECT_URI environment variables")

    # MSAL setup
    msal_app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET,
    )

    @app.server.route('/login')
    def login():
        #intiialise session state
        session["state"] = os.urandom(24).hex()
        auth_url = msal_app.get_authorization_request_url(
            SCOPE,
            state=session["state"],
            redirect_uri=REDIRECT_URI
        )
        return redirect(auth_url)

    @app.server.route('/.auth/login/aad/callback')
    def authorized():
        if request.args.get('state') != session.get("state"):
            return redirect('/login')

        if "error" in request.args or "code" not in request.args:
            return "Login failed: " + request.args.get("error", "")

        code = request.args['code']
        result = msal_app.acquire_token_by_authorization_code(
            code,
            scopes=SCOPE,
            redirect_uri=REDIRECT_URI,
        )

        if "access_token" in result:
            un=result.get("id_token_claims")
            username = un.get('preferred_username', 'User')
            session["user"] = un
            return redirect(f'/?un={username}')
            """
            #----filter based on allowed user email IDs----
            #user_email = session["user"].get("preferred_username") 
            if user_email in ALLOWED_USERS:
                return redirect('/')
            else:
                return "You are not authorized to access this application."
            """
        return "Could not acquire token: " + result.get("error_description", "")

    @app.server.route('/logout')
    def logout():
        session.clear()
        logout_url=f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/logout?post_logout_redirect_uri={REDIRECT_URI}"
        return redirect(logout_url)

"""    @app.callback(Output('page-content', 'children'), [Input('url', 'pathname')])
    def display_page(pathname):
        return html.Div([
            html.H1(f"Hello, {user_info['name']}!"),
            html.P(f"Email: {user_info['preferred_username']}"),
            html.P(dcc.Link('Logout', href='/logout'))
        ])
        
if __name__ == '__main__':
      """


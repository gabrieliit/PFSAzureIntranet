import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import msal
from flask import redirect, request, session
import index
import logging

# Load environment variables
CLIENT_ID = os.environ['MSFT_AUTH_CLIENT_ID']
CLIENT_SECRET = os.environ['MSFT_AUTH_CLIENT_SECRET']
TENANT_ID=os.environ['MSFT_AUTH_TENANT_ID']
AUTHORITY = F'https://login.microsoftonline.com/{TENANT_ID}'
REDIRECT_URI = os.environ['MSFT_AUTH_REDIRECT_URI']
SCOPE = ["User.Read"]
logging.info(CLIENT_ID)

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


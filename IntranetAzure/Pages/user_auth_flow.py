import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import msal
from flask import redirect, request, session
import Pages.index as index
import logging

"""
THe user auth flow works as follows:
1. user presses the login button and gets navigated to /login route
2. The /login route handler queries msal library to fetch microsoft auth url and creates a post request to that url inclduing the session state and redirect URI as a param
3. If the user is configured in the global directory for the app registration in MS EntraID (ie Active Dir), The Mircosft oauth provider authenticates the user (either by send ing a code to their email, or autheticator app)
4. If authentication is successful, the oauth provider sends a post request to the redirect URI, after validating the URI provided in Step 2 is the same as that configured in the Azure portal for security purposes
5. the redirect URI route handler reads the access token from the post request, extracts the user name and redirects to the home page with logged in status.
6. If oauth provider post has an error, redirect URI route handler, sends user to /login route with error message received in the post request from oauth provider
"""

# Load environment variables
CLIENT_ID = os.environ['MSFT_AUTH_CLIENT_ID']
CLIENT_SECRET = os.environ['MSFT_AUTH_CLIENT_SECRET']
TENANT_ID=os.environ['MSFT_AUTH_TENANT_ID']
AUTHORITY = f'https://login.microsoftonline.com/{TENANT_ID}'
REDIRECT_URI = os.environ['MSFT_AUTH_REDIRECT_URI']
SCOPE = ["User.Read"]

logging.basicConfig(level=logging.DEBUG,filename="user_auth_flow_log.txt",filemode="w",format='%(asctime)s %(levelname)-8s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
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
        logging.info(f"Redirect URI stored in env : {REDIRECT_URI}")
        logging.info(f"Received auth url from msal app : {auth_url}")
        return redirect(auth_url)

    @app.server.route('/.auth/login/aad/callback')
    def authorized():
        logging.info("Auth provider redirected to redirect URI")
        if request.args.get('state') != session.get("state"):
            logging.error("State variable did not match in authorisation url response")
            return redirect('/login')

        if "error" in request.args or "code" not in request.args:
            error_msg="Login failed: " + request.args.get("error", "")
            logging.error(f"Error in login request response from oauth provider : {error_msg}")
            return redirect(f'/login?error={error_msg}')

        code = request.args['code']
        logging.info(f"Received Auth code from auth URL : {code}")
        #print(code)
        result = msal_app.acquire_token_by_authorization_code(
            code,
            scopes=SCOPE,
            redirect_uri=REDIRECT_URI,
        )

        if "access_token" in result:
            un=result.get("id_token_claims")
            username = un.get('preferred_username', 'User')
            session["user"] = un
            logging.info(f"Received access token in response.  USername extracted {username}")
            return redirect(f'/?un={username}')

        error_msg= "Could not acquire token: " + result.get("error_description", "")
        logging.error(f"Failed to acquire token using auth code due to error : {error_msg}")
        return redirect(f'/login?error={error_msg}')

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


import dash
from Layouts.user_input import layout, register_callbacks

dash_obj = dash.Dash(__name__)
app = dash_obj.server #default azure guincorn startup script target a variable called app to load webapp

# Set the layout
app.layout = layout

# Register callbacks
register_callbacks(app)

if __name__ == '__main__':
    app.run_server(debug=True)



"""from flask import Flask
app= Flask(__name__)
@app.route("/")
def home():
    return "Hello World - Pushing code from PC to Azure Devops Repo"

if __name__=="__main__":
    app.run(debug=True)
"""


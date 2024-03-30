import dash
from Layouts.user_input import layout, register_callbacks

app = dash.Dash(__name__)
server = app.server

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


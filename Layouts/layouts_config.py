from Layouts import homepage, scratch,user_auth_flow,index
def register_layout_callbacks(app):
    homepage.register_callbacks(app)
    scratch.register_callbacks(app)
    user_auth_flow.register_callbacks(app)

def validate_layouts():
    #return a list of all layouts for layout validation in the app
    return [
        homepage.draw_page_content(),
        scratch.draw_page_content(),
        index.draw_page_outline(),
    ]
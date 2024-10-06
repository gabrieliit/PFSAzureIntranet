from Layouts import homepage, scratch,user_auth_flow,index,dataloader,load_jobs_inv
def register_layout_callbacks(app):
    homepage.register_callbacks(app)
    scratch.register_callbacks(app)
    user_auth_flow.register_callbacks(app)
    dataloader.register_callbacks(app)
    load_jobs_inv.register_callbacks(app)

def validate_layouts():
    #return a list of all layouts for layout validation in the app
    return [
        homepage.draw_page_content(),
        scratch.draw_page_content(),
        index.draw_page_outline(),
        dataloader.draw_page_content(),
        load_jobs_inv.draw_page_content()
    ]
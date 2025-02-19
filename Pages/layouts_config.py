from Pages import scratch, transactions_layout,user_auth_flow,index,accounts,customers,kri_layout
from Pages.Home import home_layout
from Pages.ManageData import dataloader, load_jobs_inv,manage_gold_prices
def register_layout_callbacks(app):
    home_layout.register_callbacks(app)
    scratch.register_callbacks(app)
    user_auth_flow.register_callbacks(app)
    dataloader.register_callbacks(app)
    load_jobs_inv.register_callbacks(app)
    manage_gold_prices.register_callbacks(app)
    accounts.register_callbacks(app),
    customers.register_callbacks(app),
    transactions_layout.register_callbacks(app)
    kri_layout.register_callbacks(app)


def validate_layouts():
    #return a list of all layouts for layout validation in the app
    return [
        home_layout.draw_page_content(),
        scratch.draw_page_content(),
        index.draw_page_outline(),
        dataloader.draw_page_content(),
        load_jobs_inv.draw_page_content(),
        manage_gold_prices.draw_page_content(),
        accounts.draw_page_content(),
        customers.draw_page_content(),
        transactions_layout.draw_page_content(),
        kri_layout.draw_page_content(),    
    ]
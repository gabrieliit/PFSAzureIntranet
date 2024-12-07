from ProducerServices.Aggregations import transactions_aggs as ta, accounts_aggs as aa, account_notings_aggs as ana, customers_aggs as ca
AggPipelines={
    "Transactions":ta.TransactionsAggs,
    "Accounts":aa.AccountsAggs,
    "AccountNotings":ana.AccountNotingsAggs,
    "Customers":ca.CustomersAggs  
}

def find_checksum_agg_col(pipeline,agg_field):
    #returns the col in the rule that is being aggregated for checksum
    for stage in pipeline:
        if list(stage.keys())[0]=="$group":#this is a grouping stage
            for stage_key,stage_val in stage["$group"].items():
                if stage_key==agg_field:
                    #this is the Col Sum op. Get the col name from the $sum op
                    return stage_val["$sum"][1:]#strip the leading $ out to get col name that is being summed in the checksum
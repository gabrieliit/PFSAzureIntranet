from ConsumerServices.DatasetTools.DatasetDefs import aggregations_base as agg
from dateutil.relativedelta import relativedelta as rd

class AccountCount(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lt": as_of_date
                    }
                }
            },#Filter out all loans with start date greater than ReportingCOB
            {
                "$match": 
                {
                    "$or": 
                    [
                        {
                            "LoanClosureDate": {"$gt": as_of_date}
                        },#For closed loans,Include all loans that have been closed after COB date
                        {
                            "LoanClosureDate": {"$exists": False}
                        }#Include all open loans
                    ]
                }
            },          
            {
                "$count":"AccountCount"
            }
        ]
        return pipe_def

class NewLoansCount1Q(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": as_of_date,
                        "$gt" : as_of_date - rd(months=3)
                    }              
                }
            },#Filter out all loans with start date in last quarter       
            {
                "$count":"AccountCount"
            }
        ]
        return pipe_def

class NewLoansSize1QPL(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": as_of_date,
                        "$gt" : as_of_date - rd(months=3)
                    }        
                }
            },#Filter out all loans with start date in last quarter     
            {
                "$group":
                {
                    "_id": "null",
                    "AvgLoanAmount": 
                    {
                        "$avg": "$LoanAmount"
                    },
                    "AvgWeight": 
                    {
                        "$avg": "$CollWeight"
                    },
                    "TotalLoanAmount":
                    {
                        "$sum": "$LoanAmount"
                    }
                }
            }
        ]
        return pipe_def
    
class NewLoansSize1QExPL(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": as_of_date,
                        "$gt" : as_of_date - rd(months=3)
                    }  
                }
            },#Filter out all loans with start date in last quarter     
            {
                "$match": 
                {
                    "LoanAmount": {"$exists": False}
                }
            },#Filter loans where loan amount is not available. These are loans which have been repaid and closed between two reporting data COB dates        
            {
                "$addFields": 
                {
                    "PrincRepaid":{"$sum": "$PrincPaymentAmounts"}
                }
            },#sum the princ payments for these loans        
            {
                "$group":
                {
                    "_id": "null",
                    "TotalPrincRepaid":
                    {
                        "$sum": "$PrincRepaid"
                    },#equal to sum of loan amounts for these loans since they have been fully repaid
                    "AvgPrincRepaid":
                    {
                        "$avg": "$PrincRepaid"
                    },#average loan amount for these loans
                }
            }
        ]
        return pipe_def

class LoanClosuresCount1Q(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanClosureDate":
                    {
                        "$lte": as_of_date,
                        "$gt" : as_of_date - rd(months=3)
                    }
                }
            },          
            {
                "$count":"AccountCount"
            }
        ]
        return pipe_def
    
class LoanClosuresSize1Q(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanClosureDate":
                    {
                        "$lte": as_of_date,
                        "$gt" : as_of_date - rd(months=3)
                    }
                }
            },          
            {
                "$group":
                {
                    "_id": "null",
                    "AvgLoanAmount": 
                    {
                        "$avg": "$LoanAmount"
                    },
                    "AvgWeight": 
                    {
                        "$avg": "$CollWeight"
                    }
                }
            }
        ]
        return pipe_def

class AccountAggsPL(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": as_of_date
                    }
                }
            },#Filter out all loans with start date greater than ReportingCOB
            {
                "$match": 
                {
                    "$or": 
                    [
                        {
                            "LoanClosureDate": {"$gt": as_of_date}
                        },#For closed loans,Include all loans that have been closed after COB date
                        {
                            "LoanClosureDate": {"$exists": False}
                        }#Include all open loans
                    ]
                }
            },
            {
                "$addFields":
                {
                    "PaidBeforeDate": #sum all princ payments before as_of_date
                    {
                        "$sum": 
                        {
                            "$map": #subset princ payments array to payments before as_of_date and sum them up
                            {
                                "input": 
                                {
                                    "$filter": #zip payments and amounts and filter out payments before as_of_date
                                    {
                                        "input": 
                                        {
                                            "$zip": 
                                            {
                                                "inputs": 
                                                ["$PrincPaymentAmounts","$PrincPaymentDates"]
                                            }
                                        },
                                        "as": "princPayment",
                                        "cond": 
                                        {
                                            "$lte": 
                                            [{"$arrayElemAt": ["$$princPayment",1]},as_of_date]
                                        }
                                    }
                                },
                                "as": "princPaymentBeforeDate",
                                "in": 
                                {"$arrayElemAt": ["$$princPaymentBeforeDate",0]}
                            }
                        }
                    }
                }            
            },
            {
                "$match":
                {
                    "LoanAmount": {"$exists": True},
                }#Filter out loans which do not have loan amounts. These are loans which have opened and closed between two reporting data COB dates so dont reflect in the Pending Loans files. 
            },    
            {
                "$addFields":
                {
                    "PrincOS": {"$subtract": ["$LoanAmount","$PaidBeforeDate"]},
                }#Sum up items in the princ payments array for loan accounts which do not have loan amounts  
            },       
            {
                "$group":
                {
                    "_id": "null",
                    "TotalPrincOSPL": 
                    {
                        "$sum": "$PrincOS"
                    },
                    "TotalCollWeightPL": 
                    {
                        "$sum": "$CollWeight"
                    }
                }
            }
        ]
        return pipe_def    

class AccountAggsExPL(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": as_of_date
                    }
                }
            },#Filter out all loans with start date greater than ReportingCOB
            {
                "$match": 
                {
                    "$or": 
                    [
                        {
                            "LoanClosureDate": {"$gt": as_of_date}
                        },#For closed loans,Include all loans that have been closed after COB date
                        {
                            "LoanClosureDate": {"$exists": False}
                        }#Include all open loans
                    ]
                }
            },
            {
                "$addFields":
                {
                    "PrincPaidBeforeDate": #sum all princ payments before as_of_date
                    {
                        "$sum": 
                        {
                            "$map": #subset princ payments array to payments before as_of_date and sum them up
                            {
                                "input": 
                                {
                                    "$filter": #zip payments and amounts and filter out payments before as_of_date
                                    {
                                        "input": 
                                        {
                                            "$zip": 
                                            {
                                                "inputs": 
                                                ["$PrincPaymentAmounts","$PrincPaymentDates"]
                                            }
                                        },
                                        "as": "princPayment",
                                        "cond": 
                                        {
                                            "$lte": 
                                            [{"$arrayElemAt": ["$$princPayment",1]},as_of_date]
                                        }
                                    }
                                },
                                "as": "princPaymentBeforeDate",
                                "in": 
                                {"$arrayElemAt": ["$$princPaymentBeforeDate",0]}
                            }
                        }
                    }
                }            
            },
            {
                "$match":
                {
                    "LoanAmount":{"$exists": False} 
                }#Loans do not exist in Pending Loans files for loans which have opened and closed between two reporting data COB dates. 
                #So these account documents will not have loan amounts, as loan amounts are sourced from pending loans files
            },
            {
                "$addFields":
                {
                    "PrincOS": {"$subtract":[{"$sum": "$PrincPaymentAmounts"},"$PrincPaidBeforeDate"]},
                }#Sum up items in the princ payments array for loan accounts which do not have loan amounnts. 
                #This is the total Loan Amount for these loans, as these loans have been repaid and closed. 
                #Then subtract the total princ paid from this to get the outstanding princ for these loans
            },
            {
                "$group":
                {
                    "_id": None,
                    "TotalPrincOSExPL": 
                    {
                        "$sum": "$PrincOS"
                    },
                    "TotalCollWeightExPL": 
                    {
                        "$sum": "$CollWeight"
                    }
                }#Aggregate princ repaid for all loans without loan amounts
            }         
        ]
        return pipe_def         
    
class LTVLimitBreachesPL(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": as_of_date
                    }
                }
            },#Filter out all loans with start date greater than ReportingCOB
            {
                "$match": 
                {
                    "$or": 
                    [
                        {
                            "LoanClosureDate": {"$gt": as_of_date}
                        },#For closed loans,Include all loans that have been closed after COB date
                        {
                            "LoanClosureDate": {"$exists": False}
                        }#Include all open loans
                    ]
                }
            },
            {
                "$addFields":
                {
                    "PaidBeforeDate": #sum all princ payments before as_of_date
                    {
                        "$sum": 
                        {
                            "$map": #subset princ payments array to payments before as_of_date and sum them up
                            {
                                "input": 
                                {
                                    "$filter": #zip payments and amounts and filter out payments before as_of_date
                                    {
                                        "input": 
                                        {
                                            "$zip": 
                                            {
                                                "inputs": 
                                                ["$PrincPaymentAmounts","$PrincPaymentDates"]
                                            }
                                        },
                                        "as": "princPayment",
                                        "cond": 
                                        {
                                            "$lte": 
                                            [{"$arrayElemAt": ["$$princPayment",1]},as_of_date]
                                        }
                                    }
                                },
                                "as": "princPaymentBeforeDate",
                                "in": 
                                {"$arrayElemAt": ["$$princPaymentBeforeDate",0]}
                            }
                        }
                    }
                }            
            },
            {
                "$match":
                {
                    "LoanAmount": {"$exists": True},
                }#Filter out loans which do not have loan amounts. These are loans which have opened and closed between two reporting data COB dates so dont reflect in the Pending Loans files. 
            },    
            {
                "$addFields":
                {
                    "PrincOS": {"$subtract": ["$LoanAmount","$PaidBeforeDate"]},
                }#Sum up items in the princ payments array for loan accounts which do not have loan amounts  
            },       
            {
                "$addFields":
                {
                    "LTV": {"$divide": ["$LoanAmount",{"$multiply": ["$CollWeight",params["GoldPrice"]]}]},
                }#Calculate LTV for each loan by dividing Loan Amount by Collateral Weight * Gold Price
            },
            {
                "$match":
                {
                    "LTV": {"$gt": params["LTVLimit"]}
                }
            },#Filter out loans where LTV is greater than the limit
            {
                #count number of loans and sum principal outstanding for these loans
                "$group":
                {
                    "_id": None,
                    "TotalPrincOS": 
                    {
                        "$sum": "$PrincOS"
                    },
                    "AccountCount": 
                    {
                        "$sum": 1
                    }
                }
            }
        ]
        return pipe_def

class LTVLimitBreachesExPL(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": as_of_date
                    }
                }
            },#Filter out all loans with start date greater than ReportingCOB
            {
                "$match": 
                {
                    "$or": 
                    [
                        {
                            "LoanClosureDate": {"$gt": as_of_date}
                        },#For closed loans,Include all loans that have been closed after COB date
                        {
                            "LoanClosureDate": {"$exists": False}
                        }#Include all open loans
                    ]
                }
            },
            {
                "$addFields":
                {
                    "PrincPaidBeforeDate": #sum all princ payments before as_of_date
                    {
                        "$sum": 
                        {
                            "$map": #subset princ payments array to payments before as_of_date and sum them up
                            {
                                "input": 
                                {
                                    "$filter": #zip payments and amounts and filter out payments before as_of_date
                                    {
                                        "input": 
                                        {
                                            "$zip": 
                                            {
                                                "inputs": 
                                                ["$PrincPaymentAmounts","$PrincPaymentDates"]
                                            }
                                        },
                                        "as": "princPayment",
                                        "cond": 
                                        {
                                            "$lte": 
                                            [{"$arrayElemAt": ["$$princPayment",1]},as_of_date]
                                        }
                                    }
                                },
                                "as": "princPaymentBeforeDate",
                                "in": 
                                {"$arrayElemAt": ["$$princPaymentBeforeDate",0]}
                            }
                        }
                    }
                }            
            },
            {
                "$match":
                {
                    "LoanAmount":{"$exists": False} 
                }#Loans do not exist in Pending Loans files for loans which have opened and closed between two reporting data COB dates. 
                #So these account documents will not have loan amounts, as loan amounts are sourced from pending loans files
            },
            {
                "$addFields":
                {
                    "PrincOS": {"$subtract":[{"$sum": "$PrincPaymentAmounts"},"$PrincPaidBeforeDate"]},
                }#Sum up items in the princ payments array for loan accounts which do not have loan amounnts. 
                #This is the total Loan Amount for these loans, as these loans have been repaid and closed.
                # Then subtract the total princ paid from this to get the outstanding princ for these loans
            },
            {
                "$addFields":
                {
                    "LTV": {"$divide": ["$PrincOS",{"$multiply": ["$CollWeight",params["GoldPrice"]]}]},
                }#Calculate LTV for each loan by dividing Loan Amount by Collateral Weight * Gold Price
            },
            {
                "$match":
                {
                    "LTV": {"$gt": params["LTVLimit"]}
                }
            },#Filter out loans where LTV is greater than the limit
            {
                #count number of loans and sum principal outstanding for these loans
                "$group":
                {
                    "_id": None,
                    "TotalPrincOS": 
                    {
                        "$sum": "$PrincOS"
                    },
                    "AccountCount": 
                    {
                        "$sum": 1
                    }
                }
            }
        ]
        return pipe_def

class LoansPastCloseDatePL(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": as_of_date
                    }
                }
            },#Filter out all loans with start date greater than ReportingCOB
            {
                "$match": 
                {
                    "$or": 
                    [
                        {
                            "LoanClosureDate": {"$gt": as_of_date}
                        },#For closed loans,Include all loans that have been closed after COB date
                        {
                            "LoanClosureDate": {"$exists": False}
                        }#Include all open loans
                    ]
                }
            },
            {
                "$addFields":
                {
                    "PaidBeforeDate": #sum all princ payments before as_of_date
                    {
                        "$sum": 
                        {
                            "$map": #subset princ payments array to payments before as_of_date and sum them up
                            {
                                "input": 
                                {
                                    "$filter": #zip payments and amounts and filter out payments before as_of_date
                                    {
                                        "input": 
                                        {
                                            "$zip": 
                                            {
                                                "inputs": 
                                                ["$PrincPaymentAmounts","$PrincPaymentDates"]
                                            }
                                        },
                                        "as": "princPayment",
                                        "cond": 
                                        {
                                            "$lte": 
                                            [{"$arrayElemAt": ["$$princPayment",1]},as_of_date]
                                        }
                                    }
                                },
                                "as": "princPaymentBeforeDate",
                                "in": 
                                {"$arrayElemAt": ["$$princPaymentBeforeDate",0]}
                            }
                        }
                    }
                }            
            },
            {
                "$match":
                {
                    "LoanAmount": {"$exists": True},
                }#Filter out loans which have Loan Amounts. These are loans exist in PL files on reporting COB dates . 
            },    
            {
                "$addFields":
                {
                    "PrincOS": {"$subtract": ["$LoanAmount","$PaidBeforeDate"]},
                }#Sum up items in the princ payments array for loan accounts which do not have loan amounts  
            },
            #Select loans which are past their close date, ie, LoanDueDate is before as_of_date. 
            # Calculate no of loans and sum of princ outstanding for these loans       
            {
                "$match":
                {
                    "LoanDueDate": {"$lt": as_of_date}
                }
            },
            {
                "$group":
                {
                    "_id": None,
                    "TotalPrincOS": 
                    {
                        "$sum": "$PrincOS"
                    },
                    "AccountCount": 
                    {
                        "$sum": 1
                    }
                }
            }
        ]
        return pipe_def
    
class LoansPastCloseDateExPL(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": as_of_date
                    }
                }
            },#Filter out all loans with start date greater than ReportingCOB
            {
                "$match": 
                {
                    "$or": 
                    [
                        {
                            "LoanClosureDate": {"$gt": as_of_date}
                        },#For closed loans,Include all loans that have been closed after COB date
                        {
                            "LoanClosureDate": {"$exists": False}
                        }#Include all open loans
                    ]
                }
            },
            {
                "$addFields":
                {
                    "PrincPaidBeforeDate": #sum all princ payments before as_of_date
                    {
                        "$sum": 
                        {
                            "$map": #subset princ payments array to payments before as_of_date and sum them up
                            {
                                "input": 
                                {
                                    "$filter": #zip payments and amounts and filter out payments before as_of_date
                                    {
                                        "input": 
                                        {
                                            "$zip": 
                                            {
                                                "inputs": 
                                                ["$PrincPaymentAmounts","$PrincPaymentDates"]
                                            }
                                        },
                                        "as": "princPayment",
                                        "cond": 
                                        {
                                            "$lte": 
                                            [{"$arrayElemAt": ["$$princPayment",1]},as_of_date]
                                        }
                                    }
                                },
                                "as": "princPaymentBeforeDate",
                                "in": 
                                {"$arrayElemAt": ["$$princPaymentBeforeDate",0]}
                            }
                        }
                    }
                }            
            },
            {
                "$match":
                {
                    "LoanAmount": {"$exists": False} 
                }#Loans do not exist in Pending Loans files for loans which have opened and closed between two reporting data COB dates. 
                #So these account documents will not have closure dates, as Loan Amounts are sourced from pending loans files
            },
            {
                "$addFields":
                {
                    "PrincOS": {"$subtract":[{"$sum": "$PrincPaymentAmounts"},"$PrincPaidBeforeDate"]},
                }#Sum up items in the princ payments array for loan accounts which do not have closure dates. 
                #This is the total Loan Amount for these loans, as these loans have been repaid and closed.
                # Then subtract the total princ paid from this to get the outstanding princ for these loans
            },
            #Select loans which are past their close date, ie, LoanDueDate is before as_of_date.
            # Calculate no of loans and sum of princ outstanding for these loans       
            {
                "$match":
                {
                    "LoanDueDate": {"$lt": as_of_date}
                }
            },
            {
                "$group":
                {
                    "_id": None,
                    "TotalPrincOS": 
                    {
                        "$sum": "$PrincOS"
                    },
                    "AccountCount": 
                    {
                        "$sum": 1
                    }
                }
            }
        ]
        return pipe_def
    
class LoansWithNotices(agg.AggPipeBuilder):
    def build_agg_pipe(self, as_of_date, params):
        pipe_def = [
            {
                "$lookup": {
                    "from": "AccountNotings",
                    "localField": "GLNo",
                    "foreignField": "GLNo",
                    "as": "AccountNotings"
                }
            },#Lookup account notings
            {
                "$unwind": {
                    "path": "$AccountNotings"
                }
            },#Flatten account notings by creating seperate records for each noting
            {
                "$match": {
                    "AccountNotings.NotingType": {
                        "$in": ["Auction", "Notice1", "Notice2"]
                    }
                }
            },#Filter out accounts with notices
            {
                "$project": {
                    "GLNo": 1,
                    "NoticeType": "$AccountNotings.NotingType",
                    "NoticeDate": "$AccountNotings.Date",
                    "LoanAmount": 1,
                    "LoanStartDate": 1,
                    "LoanClosureDate": 1,
                    "PrincPaymentAmounts": 1,
                    "PrincPaymentDates": 1
                }
            },#project fields
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": as_of_date
                    }
                }
            },#Filter out all loans with start date greater than ReportingCOB
            {
                "$match": 
                {
                    "$or": 
                    [
                        {
                            "LoanClosureDate": {"$gt": as_of_date}
                        },#For closed loans,Include all loans that have been closed after COB date
                        {
                            "LoanClosureDate": {"$exists": False}
                        }#Include all open loans
                    ]
                }
            },
            {
                "$addFields":
                {
                    "PrincPaidBeforeDate": #sum all princ payments before as_of_date
                    {
                        "$sum": 
                        {
                            "$map": #subset princ payments array to payments before as_of_date and sum them up
                            {
                                "input": 
                                {
                                    "$filter": #zip payments and amounts and filter out payments before as_of_date
                                    {
                                        "input": 
                                        {
                                            "$zip": 
                                            {
                                                "inputs": 
                                                ["$PrincPaymentAmounts","$PrincPaymentDates"]
                                            }
                                        },
                                        "as": "princPayment",
                                        "cond": 
                                        {
                                            "$lte": 
                                            [{"$arrayElemAt": ["$$princPayment",1]},as_of_date]
                                        }
                                    }
                                },
                                "as": "princPaymentBeforeDate",
                                "in": 
                                {"$arrayElemAt": ["$$princPaymentBeforeDate",0]}
                            }
                        }
                    }
                }            
            },
            {
                "$addFields":
                {
                    "PrincOS": {"$subtract":["$LoanAmount","$PrincPaidBeforeDate"]},
                }#Sum up items in the princ payments array for loan accounts which do not have closure dates. 
                #This is the total Loan Amount for these loans, as these loans have been repaid and closed.
                # Then subtract the total princ paid from this to get the outstanding princ for these loans
            },
            # Calculate no of loans and sum of princ outstanding for these loans       
            {
                "$group":
                {
                    "_id": None,
                    "TotalPrincOS": 
                    {
                        "$sum": "$PrincOS"
                    },
                    "AccountCount": 
                    {
                        "$sum": 1
                    }
                }
            }
        ]
        return pipe_def

class DailyOutflows(agg.AggPipeBuilder):
    def build_agg_pipe(self,as_of_date,params):
        pipe_def=[
            {
                "$match": 
                {
                    "LoanStartDate": 
                    {
                        "$lte": params["EndDate"],
                        "$gte" : params["StartDate"]
                    }        
                }
            },#Filter out all Loans disbursed between start and end date 
            {
                #Add attrib LoanAmount_PLExPL, populated with LoanAmount attrib if it exists in the doc, else by sum of PrincPayments attribute
                {
                    "$addFields": {
                        "LoanAmount_PLExPL": {
                            "$cond": {
                                "if": {"$gt":["$LoanAmount",0]} ,
                                "then": "$LoanAmount",
                                "else": {"$sum": "$PrincPaymentAmounts"}
                            }
                        }
                    }
                }
            },
            {
                "$group":
                {
                    "_id": "$LoanStartDate",
                    "TotalOutflows": 
                    {
                        "$sum": "$LoanAmount_PLExPL"
                    },
                }
            },#Sum up daily outflows
            {
                #project id field as LoanStartDate, and drop _id field
                {
                    "$project": {
                        "Date": "$_id",
                        "_id": 0,
                        "TotalOutflows": 1
                    }
                }
            }

        ]
        return pipe_def
class AggPipeBuilder():
    def __init__(self,as_of_date=None,agg_params={}):
        self.agg_pipe_def=self.build_agg_pipe(as_of_date,agg_params)
    
    def build_agg_pipe(self,as_of_date=None,params={}):
        return [] #this is the interface class. Method should be implemented in the child class 

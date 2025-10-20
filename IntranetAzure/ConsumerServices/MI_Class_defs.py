import os
from SourceObj_Classes import DataSource
from DataServices.Extract.source_metadata import Sources
from MI_metadata import MI_facts_metadata
import numpy as np
import pandas as pd

def compare(a, op, b):
    try:
        if op=="<":
            return a<b
        elif op=="=":
            return a==b
        elif op==">":
            return a > b
        elif op=="!=":
            return a != b
    except:
        print("hi")

class MetricFactory:

    def __init__(self,metrics,run_config):
        """
        Job_q structure
        {
            dataset:
            {
                SourceCalcJobs:
                {
                    source:
                    {
                        job_name:
                        {
                            "type": "calc_metric",
                            "calc_type":the calc type defined for the metric in MI_facts_metadata,
                            "CalcAttributes":the calc attribute defined for the metric in MI_facts_metadata,
                            "metric_name": maps to a metric defined in MI_facts_metadata
                            "result": result of the metric calc
                        }
                    },
                },
                agg_jobs:
                {
                    job_name:
                    {
                        "type": "source_agg_metric",
                        "source_agg_type":the source agg type defined for the metric in MI_facts_metadata,
                        "metric_name": maps to a metric defined in MI_facts_metadata,
                        "sources": [list of sources for the metric]
                        "PostAggJobs":any post agg ops to be applied
                        "job_outputs":
                        {
                            "calc_result": calc result
                            "custom_cols": custom cols for each result
                        }
                    }
                }
            }
        }
        :return:
        """
        datasets=[run_config["Dataset"]]+run_config["TrendDatasets"]
        self.datasets=datasets
        self.metrics=metrics
        self.run_config=run_config
        #create source wise job queue for each dataset
        job_q={}
        for dataset in datasets:
            job_q[dataset]={}
            job_q[dataset]["SourceCalcJobs"]={}
            job_q[dataset]["agg_jobs"] = {}
            for source in Sources:#initialise an empty jobs list for each dataset and source
                job_q[dataset]["SourceCalcJobs"][source]={}
            for metric in metrics:
                source_list=MI_facts_metadata[metric]["Source"]
                sa_job_name = f"sa_job_{dataset}_{metric}"
                try:
                    post_agg_jobs = MI_facts_metadata[metric]["PostAggJobs"]
                except KeyError:
                    post_agg_jobs = None
                try:
                    source_agg_type=MI_facts_metadata[metric]["SourceAggType"]
                    sa_job_detail={"type":"source_agg","metric_name":metric,"sources":source_list,"source_agg_type":source_agg_type,"PostAggJobs":post_agg_jobs}
                    job_q[dataset]["agg_jobs"][sa_job_name]=sa_job_detail
                except KeyError: #no source agg_type defined for the metric, ie just one source
                    job_q[dataset]["agg_jobs"][sa_job_name]={"type":"source_agg","metric_name":metric,"sources":source_list,"source_agg_type":"NA","PostAggJobs":post_agg_jobs}

                for source in source_list:#add jobs for each dataset for each source for which the metric is configured in MI_facts_metadata
                    job_name=f"cm_job_{dataset}_{source}_{metric}"
                    job_detail=\
                    {
                        "type":"calc_metric",
                        "metric_name":metric,
                        "calc_type":MI_facts_metadata[metric]["CalcType"],
                        "CalcAttributes":MI_facts_metadata[metric]["CalcAttributes"]
                    } #follow dictionary naming convention
                    try:
                        job_detail["Filters"]=MI_facts_metadata[metric]["Filters"]
                    except KeyError:
                        pass
                    job_q[dataset]["SourceCalcJobs"][source][job_name]=job_detail
            self.job_q=job_q

    def calc_metrics(self):
        #process all jobs in job_q
        for dataset in self.job_q.keys():
            for source,jobs in self.job_q[dataset]["SourceCalcJobs"].items():#first process all calc jobs for each metric for each source
                if len(jobs)>0:
                    #create source object
                    input_folder = os.path.join('MI/Data', dataset)
                    source_obj = DataSource.datasource_factory(input_folder, source,self.run_config,dataset)
                    # Load data
                    try:
                        source_obj.load_data(skiprows=Sources[source]["SkipRows"], col_names=Sources[source]["ColNames"])
                    except KeyError as e:
                        if e.args[0]=="ColNames":#no col_names provided
                            source_obj.load_data(skiprows=Sources[source]["SkipRows"])
                        elif e.args[0]=="SkipRows":#Skip rows can only be skipped when load is called by a Compound data source object. Signature in this case doesnt need any args
                            source_obj.load_data()

                    for job_name,job_details in jobs.items():
                        calc_type=job_details["calc_type"]
                        source_obj.df["Include"] = True
                        if job_details["metric_name"]=="TXV02_Loans closed per day (15 week avg)":
                            print("hi")
                        #calcualte filters
                        try:#if filters are defined for the metric
                            for filter in job_details["Filters"]:
                                p_val = filter["PartitionVal"]
                                p_col = filter["PartitionCol"]
                                p_cond= compare(source_obj.df[p_col], filter["Condition"], p_val)
                                source_obj.df["Include"] &= p_cond
                        except KeyError:
                            pass
                        if job_details["type"]=="calc_metric":
                            if calc_type=="list_unique":
                                job_details["result"]=source_obj.df[source_obj.df["Include"]][job_details["CalcAttributes"][0]].dropna().unique()
                            elif calc_type=="col_sum":
                                job_details["result"] = source_obj.df[source_obj.df["Include"]][job_details["CalcAttributes"][0]].dropna().sum()
                            elif calc_type=="col_avg":
                                job_details["result"] = source_obj.df[source_obj.df["Include"]][job_details["CalcAttributes"][0]].dropna().mean()
                            elif calc_type=="ratio_col_sum":
                                job_details["result"] = source_obj.df[source_obj.df["Include"]][job_details["CalcAttributes"]["Num"]].dropna().sum()/source_obj.df[job_details["CalcAttributes"]["Denom"]].dropna().sum()
                            elif calc_type=="diff_of_cols":
                                job_details["result"] = source_obj.df[source_obj.df["Include"]][job_details["CalcAttributes"]["Col1"]].dropna() - source_obj.df[job_details["CalcAttributes"]["Col2"]].dropna()
                            elif calc_type=="partition_proportion":
                                df=source_obj.df
                                attrib=job_details["CalcAttributes"]
                                result=(df[df["Include"]][attrib].sum() / df[attrib].sum()*100)[0]
                                job_details["result"]=result
                            elif calc_type=="wtd_avg":
                                df = source_obj.df
                                vals=df[job_details["CalcAttributes"]["Vals"]]
                                weights = df[job_details["CalcAttributes"]["Weights"]]
                                job_details["result"] = (vals*weights).sum()/weights.sum()
                            else:
                                job_details["result"]=f"Job handler not found in Metric Factory class for calc_type = {calc_type} "
            for job_name,job_details in self.job_q[dataset]["agg_jobs"].items():#now process the agg_jobs
                job_details["job_outputs"]={}
                source_agg_type=job_details["source_agg_type"]
                if source_agg_type=="NA": #no source agg job, ie  just one source defined
                    source=job_details["sources"][0]
                    metric = job_details["metric_name"]
                    job_details["job_outputs"]["calc_result"]=self.job_q[dataset]["SourceCalcJobs"][source][f"cm_job_{dataset}_{source}_{metric}"]["result"]
                    job_details["job_outputs"]["comment"]=f"Source - {source}"
                elif source_agg_type =="count_unique":#count unique items across lists for each source
                    agg_list=[]
                    for source in job_details["sources"]:#concatenate lists across sources
                        metric=job_details["metric_name"]
                        agg_list=np.unique(np.concatenate((agg_list,self.job_q[dataset]["SourceCalcJobs"][source][f"cm_job_{dataset}_{source}_{metric}"]["result"])))
                    job_details["job_outputs"]["calc_result"]=len(agg_list)
                    sources = job_details["sources"]
                    try:
                        job_details["job_outputs"]["comment"] = f"count of unique from {sources}."
                    except:
                        job_details["job_outputs"]["comment"]=f"count of unique from {sources}"
                elif source_agg_type=="max":
                    result=float("-inf")#start with -inf and update the max value across sources
                    result_source="NA"
                    for source in job_details["sources"]:#concatenate lists across sources
                        metric=job_details["metric_name"]
                        if (self.job_q[dataset]["SourceCalcJobs"][source][f"cm_job_{dataset}_{source}_{metric}"]["result"] > result):
                            result = self.job_q[dataset]["SourceCalcJobs"][source][f"cm_job_{dataset}_{source}_{metric}"]["result"]
                            result_source = source
                    job_details["job_outputs"]["calc_result"]=result
                    job_details["job_outputs"]["source"]=result_source
                    job_details["job_outputs"]["comment"]=f"source = {result_source}"
                elif source_agg_type=="avg":
                    result=0
                    sources=job_details["sources"]
                    for source in sources:
                        metric=job_details["metric_name"]
                        try:
                            result += self.job_q[dataset]["SourceCalcJobs"][source][f"cm_job_{dataset}_{source}_{metric}"]["result"]
                        except:
                            pass
                    job_details["job_outputs"]["calc_result"]=result/len(sources)
                    job_details["job_outputs"]["comment"] = f"avg of avg from {sources}"
                else:
                    job_details["job_outputs"]["calc_result"] = f"Job handler not found in Metric Factory class for source_agg_type = {source_agg_type}"
                #process post agg jobs
                post_agg_jobs=job_details["PostAggJobs"]
                if post_agg_jobs:
                    for job in post_agg_jobs:
                        if job["JobType"]=="format_str":
                            format_str=job["Format"]
                            job_details["job_outputs"]["calc_result"]=format_str.format(job_details["job_outputs"]["calc_result"])
                        elif job["JobType"]=="div":
                            denom=job["Denom"]
                            job_details["job_outputs"]["calc_result"] /=denom
                        elif job["JobType"]=="mean":
                            job_details["job_outputs"]["calc_result"]=job_details["job_outputs"]["calc_result"].mean()
                        elif job["JobType"]=="time_delta_to_days":
                            job_details["job_outputs"]["calc_result"] = job_details["job_outputs"]["calc_result"].total_seconds()/86400
                #check if output format is specified
                try:
                    job_details["job_outputs"]["DisplayFormat"]=MI_facts_metadata[metric]["DisplayFormat"]
                except KeyError:
                    job_details["job_outputs"]["DisplayFormat"] = None


if __name__=="__main__":
    #   Perform Trend analysis
    datasets=["Oct23","Apr24"]
    metrics=["LP01_No of unique GL"]
    mi_run_params = {
        "CollRate": 6705.0,
        "Dataset": "Apr24",
        "RepDate": pd.to_datetime('2024-04-16'),
        "TrendDatasets":["Oct23"]
    }

    mf=MetricFactory(datasets,metrics,mi_run_params)
    mf.calc_metrics()
    print(mf.job_q)
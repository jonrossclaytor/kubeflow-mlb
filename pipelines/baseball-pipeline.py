#!/usr/bin/env python3

import kfp
from kfp import dsl
import kfp.gcp as gcp


def collect_stats_op(): #symbol
    return dsl.ContainerOp(
        name='Collect Stats',
        image='gcr.io/ross-kubeflow/collect-stats:latest'       
        
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))


def feature_eng_op(): 
    return dsl.ContainerOp(
        name='Feature Engineering',
        image='gcr.io/ross-kubeflow/feature-eng:latest'       
        
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))


def train_test_val_op(pitch_type): 
    return dsl.ContainerOp(
        name='Split Train Test Val',
        image='gcr.io/ross-kubeflow/train-test-val:latest',
        arguments=[
            '--pitch_type', pitch_type
        ]    
        
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))


def tune_hp_op(pitch_type): 
    return dsl.ContainerOp(
        name='Tune Hyperparameters',
        image='gcr.io/ross-kubeflow/tune-hp:latest',
        arguments=[
            '--pitch_type', pitch_type
        ]    
        
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))


def train_xgboost_op(pitch_type): 
    return dsl.ContainerOp(
        name='Train XGBoost',
        image='gcr.io/ross-kubeflow/train-xgboost:latest',
        arguments=[
            '--pitch_type', pitch_type
        ]    
        
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))


def host_xgboost_op(pitch_type): 
    return dsl.ContainerOp(
        name='Host Model',
        image='gcr.io/ross-kubeflow/host-xgboost:latest',
        arguments=[
            '--pitch_type', pitch_type
        ] 
        
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))


def find_threshold_op(pitch_type): 
    return dsl.ContainerOp(
        name='Find Threshold',
        image='gcr.io/ross-kubeflow/find-threshold:latest',
        arguments=[
            '--pitch_type', pitch_type
        ]    
        
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))


def evaluate_model_op(pitch_type): 
    return dsl.ContainerOp(
        name='Eval',
        image='gcr.io/ross-kubeflow/evaluate-model:latest',
        arguments=[
            '--pitch_type', pitch_type
        ]      
        
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))



@dsl.pipeline(
    name='Sequential pipeline',
    description='A pipeline with two sequential steps.'
)
def sequential_pipeline():  
    """A pipeline with sequential steps.""" 


    refresh_data_pipeline = feature_eng_op().after(collect_stats_op())
    all_pitchtypes = ['FT','FS','CH','FF','SL','CU','FC','SI','KC','EP','KN','FO']

    for pitch_type in all_pitchtypes:
        train_test_val_task = evaluate_model_op(pitch_type).after(find_threshold_op(pitch_type).after(host_xgboost_op(pitch_type).after(train_xgboost_op(pitch_type).after(tune_hp_op(pitch_type).after(train_test_val_op(pitch_type).after(refresh_data_pipeline))))))


if __name__ == '__main__':
    kfp.compiler.Compiler().compile(sequential_pipeline, __file__ + '.zip')
#!/usr/bin/env python3

import kfp
from kfp import dsl
import kfp.gcp as gcp


def collect_stats_op(): #symbol
    return dsl.ContainerOp(
        name='Collect Stats',
        image='gcr.io/ross-kubeflow/find-threshold:latest'       
        #arguments=[
        #    '--symbol', symbol
        #]
        
        #file_outputs={
        #    'output': '/usr/src/app/output.csv',
        #}
        
    ).apply(gcp.use_gcp_secret('user-gcp-sa'))


@dsl.pipeline(
    name='Sequential pipeline',
    description='A pipeline with two sequential steps.'
)
def sequential_pipeline():  #dsl.PipelineParam(name='symbol', value='GOOG')
    """A pipeline with two sequential steps."""

    collect_stats_task = collect_stats_op() #symbol

if __name__ == '__main__':
    kfp.compiler.Compiler().compile(sequential_pipeline, __file__ + '.zip')
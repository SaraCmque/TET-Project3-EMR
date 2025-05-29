import boto3
import time

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('EmrClusterLock')


def create_emr_with_steps():
    emr = boto3.client('emr', region_name='us-east-1')
    response = emr.run_job_flow(
        Name='ClimateTrafficCluster',
        ReleaseLabel='emr-7.3.0',
        Applications=[
            {'Name': 'Spark'},
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Principal",
                    'Market': 'SPOT',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': "Central",
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2
                }
            ],
            'Ec2KeyName': 'vockey',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False
        },
        Configurations=[
            {
                "Classification": "hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            {
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            }
        ],
        BootstrapActions=[
            {
                'Name': 'InstallVisualizationPackages',
                'ScriptBootstrapAction': {
                    'Path': 's3://eafit-project-3-bucket/scripts/install.sh',
                    'Args': []
                }
            }
        ],
        LogUri='s3://eafit-project-3-bucket/emr-logs/',
        Steps=[
            {
                'Name': 'TransformRawToTrusted',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        's3://eafit-project-3-bucket/scripts/transform_to_trusted.py'
                    ]
                }
            },
             {
                'Name': 'DataAnalysis',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        's3://eafit-project-3-bucket/scripts/analysis.py'
                    ]
                }
            }
        ],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
        VisibleToAllUsers=True,
        AutoTerminationPolicy={
            'IdleTimeout': 3600
        },
        Tags=[
            {'Key': 'Environment', 'Value': 'Development'},
            {'Key': 'Project', 'Value': 'DataProcessing'}
        ]
    )
    return response['JobFlowId']
 
def lambda_handler(event, context):
    if 'Records' in event and event['Records'][0]['eventSource'] == 'aws:s3':
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print(f"Nuevo archivo detectado: s3://{bucket}/{key}")
        if key.startswith('raw/'):
            response = table.get_item(Key={'LockId': 'emr_job'})
            if 'Item' in response:
                print("Cluster EMR ya iniciado, se ignora esta ejecución")
                return {'statusCode': 200, 'body': 'Cluster EMR ya iniciado'}
            else:
                expiration_time = int(time.time()) + 120
                table.put_item(Item={
                    'LockId': 'emr_job',
                    'ExpiresAt': expiration_time
                })
                emr = boto3.client('emr')
                cluster_id = create_emr_with_steps()
                return {
                    'statusCode': 200,
                    'body': f'Cluster EMR {cluster_id} iniciado por nuevo archivo en {key}'
                }
    return {
        'statusCode': 400,
        'body': 'Evento no válido'
}
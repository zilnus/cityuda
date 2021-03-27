import configparser
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (LoadFileIntoS3Operator, DataQualityOperator,
                               ParquetToRedshiftOperator)
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

# Configurations
config = configparser.ConfigParser()
config.read(os.path.join('/home/workspace/airflow/dags', 'capstone.cfg'))

SPARK_STEPS = [
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/data",
                "--dest=/input",
            ],
        },
    },
    {
        "Name": "Capstone Data Transformation",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--packages",
                "saurfang:spark-sas7bdat:2.0.0-s_2.11",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
            ],
        },
    },
]

JOB_FLOW_OVERRIDES = {
    "Name": "Capstone cluster",
    "ReleaseLabel": "emr-5.28.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3", "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core Node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": "s3://udacity-capstone-satria/logs/",
    "VisibleToAllUsers": True
}

# Default argument of Airflow DAG
default_args = {
    "owner": "satria",
    "depends_on_past": False,
    "start_date": datetime(2021, 3, 26),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# create DAG
dag = DAG(
    "etl_capstone_final",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    max_active_runs=1
)

# Dummy Start
start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

# Load immigration file into S3
immigration_to_s3 = LoadFileIntoS3Operator(
    dag=dag,
    task_id="immigration_to_s3",
    airflow_folder = config['S3']['airflow_folder'],
    filename = config['S3']['local_immigration'],
    s3_key = config['S3']['s3_immigration'],
    bucket_name = config['S3']['BUCKET_NAME'],
    aws_credentials_id = "aws_credentials"
)

# Load global temperature file into S3
temperature_to_s3 = LoadFileIntoS3Operator(
    dag=dag,
    task_id="temperature_to_s3",
    airflow_folder = config['S3']['airflow_folder'],
    filename = config['S3']['local_temperature'],
    s3_key = config['S3']['s3_temperature'],
    bucket_name = config['S3']['BUCKET_NAME'],
    aws_credentials_id = "aws_credentials"
)

# Load demographic file into S3
demographic_to_s3 = LoadFileIntoS3Operator(
    dag=dag,
    task_id="demographic_to_s3",
    airflow_folder = config['S3']['airflow_folder'],
    filename = config['S3']['local_demo'],
    s3_key = config['S3']['s3_demo'],
    bucket_name = config['S3']['BUCKET_NAME'],
    aws_credentials_id = "aws_credentials"
)

# Load country file into S3
country_to_s3 = LoadFileIntoS3Operator(
    dag=dag,
    task_id="country_to_s3",
    airflow_folder = config['S3']['airflow_folder'],
    filename = config['S3']['local_country'],
    s3_key = config['S3']['s3_country'],
    bucket_name = config['S3']['BUCKET_NAME'],
    aws_credentials_id = "aws_credentials"
)

# Load ports file into S3
port_to_s3 = LoadFileIntoS3Operator(
    dag=dag,
    task_id="port_to_s3",
    airflow_folder = config['S3']['airflow_folder'],
    filename = config['S3']['local_port'],
    s3_key = config['S3']['s3_port'],
    bucket_name = config['S3']['BUCKET_NAME'],
    aws_credentials_id = "aws_credentials"
)

# Load state file into S3
state_to_s3 = LoadFileIntoS3Operator(
    dag=dag,
    task_id="state_to_s3",
    airflow_folder = config['S3']['airflow_folder'],
    filename = config['S3']['local_state'],
    s3_key = config['S3']['s3_state'],
    bucket_name = config['S3']['BUCKET_NAME'],
    aws_credentials_id = "aws_credentials"
)

# Load pyspark script file into S3
script_to_s3 = LoadFileIntoS3Operator(
    dag=dag,
    task_id="script_to_s3",
    airflow_folder = config['S3']['airflow_folder'],
    filename = config['S3']['local_script'],
    s3_key = config['S3']['s3_script'],
    bucket_name = config['S3']['BUCKET_NAME'],
    aws_credentials_id = "aws_credentials"
)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_credentials",
    emr_conn_id="emr_default",
    dag=dag,
)

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": config['S3']['BUCKET_NAME'],
        "s3_script": config['S3']['s3_script'],
        "s3_clean": config['S3']['s3_clean'],
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_credentials",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    dag=dag,
)

# load immigration parquet to redshift
immig_to_redshift = ParquetToRedshiftOperator(
    task_id='Stage_immigration',
    dag=dag,
    table="immigrant",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-satria",
    s3_key="clean_data/immigration.parquet"    
)

# load country parquet to redshift
country_to_redshift = ParquetToRedshiftOperator(
    task_id='Stage_country',
    dag=dag,
    table="country",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-satria",
    s3_key="clean_data/country.parquet"    
)

# load port parquet to redshift
port_to_redshift = ParquetToRedshiftOperator(
    task_id='Stage_port',
    dag=dag,
    table="port",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-satria",
    s3_key="clean_data/port.parquet"    
)

# load mode transport parquet to redshift
modetrans_to_redshift = ParquetToRedshiftOperator(
    task_id='Stage_Mode_Transport',
    dag=dag,
    table="mode_transport",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-satria",
    s3_key="clean_data/mode_transport.parquet"    
)

# load visa category parquet to redshift
viscat_to_redshift = ParquetToRedshiftOperator(
    task_id='Stage_Visa_Category',
    dag=dag,
    table="visa_category",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-satria",
    s3_key="clean_data/visa_category.parquet"    
)

# load calendar parquet to redshift
calendar_to_redshift = ParquetToRedshiftOperator(
    task_id='Stage_Calendar',
    dag=dag,
    table="calendar",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-satria",
    s3_key="clean_data/calendar.parquet"    
)

# load state parquet to redshift
state_to_redshift = ParquetToRedshiftOperator(
    task_id='Stage_State',
    dag=dag,
    table="state",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-satria",
    s3_key="clean_data/state.parquet"    
)

# define data quality check task
dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM immigrant WHERE cicid IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM country WHERE country_code IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM port WHERE port_code IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM mode_transport WHERE mode_code IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM visa_category WHERE visacat_code IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM calendar WHERE date_deparr IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM state WHERE state_code IS NULL", 'expected_result': 0}
    ]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",    
    dq_checks=dq_checks
)

# Dummy End
end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

# Create DAG graph

# Load data into S3
start_data_pipeline >> immigration_to_s3
start_data_pipeline >> temperature_to_s3
start_data_pipeline >> demographic_to_s3
start_data_pipeline >> country_to_s3
start_data_pipeline >> port_to_s3
start_data_pipeline >> script_to_s3
start_data_pipeline >> state_to_s3

# Finish load data to S3
immigration_to_s3 >> create_emr_cluster
temperature_to_s3 >> create_emr_cluster
demographic_to_s3 >> create_emr_cluster
state_to_s3 >> create_emr_cluster
country_to_s3 >> create_emr_cluster
port_to_s3 >> create_emr_cluster
script_to_s3 >> create_emr_cluster

# Create EMR, perform ETL process, terminate EMR
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster

# After terminate EMR, load parquet from S3 to Redshift
terminate_emr_cluster >> immig_to_redshift
terminate_emr_cluster >> country_to_redshift
terminate_emr_cluster >> port_to_redshift
terminate_emr_cluster >> modetrans_to_redshift
terminate_emr_cluster >> viscat_to_redshift
terminate_emr_cluster >> calendar_to_redshift
terminate_emr_cluster >> state_to_redshift

# Run Data Quality Check
immig_to_redshift     >> run_quality_checks
country_to_redshift   >> run_quality_checks
port_to_redshift      >> run_quality_checks
modetrans_to_redshift >> run_quality_checks
viscat_to_redshift    >> run_quality_checks
calendar_to_redshift  >> run_quality_checks
state_to_redshift     >> run_quality_checks

# End pipeline
run_quality_checks >> end_data_pipeline


import json
import boto3
import sagemaker
from sagemaker.workflow.parameters import (
    ParameterInteger,
    ParameterString,
)
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.properties import PropertyFile
from sagemaker.processing import (
    ProcessingInput,
    ProcessingOutput,
    ScriptProcessor,
)
from sagemaker.workflow.condition_step import (
    ConditionStep,
    JsonGet,
)
from sagemaker.workflow.conditions import ConditionGreaterThanOrEqualTo
from sagemaker.workflow.execution_variables import ExecutionVariables
from datetime import datetime

# Configuración básica
base_job_prefix = "CocaCola-RFM"
region = boto3.session.Session().region_name
role = "REPLACE_WITH_SAGEMAKER_EXECUTION_ROLE_ARN"

# Inicializar SageMaker session
sagemaker_session = sagemaker.Session()
s3_bucket = sagemaker_session.default_bucket()
default_bucket = f"s3://{s3_bucket}"

# Obtener fecha actual para versionado
current_time = datetime.now()
date_str = current_time.strftime("%Y-%m-%d")

# Definir parámetros del pipeline
processing_instance_type = ParameterString(
    name="ProcessingInstanceType",
    default_value="ml.m5.xlarge"
)

processing_instance_count = ParameterInteger(
    name="ProcessingInstanceCount",
    default_value=1
)

input_data = ParameterString(
    name="InputDataS3Uri",
    default_value=f"{default_bucket}/coca-cola/curated/customer_orders/"
)

model_output = ParameterString(
    name="ModelOutputS3Uri",
    default_value=f"{default_bucket}/coca-cola/models/rfm/"
)

n_clusters = ParameterInteger(
    name="NumberOfClusters",
    default_value=5
)

k_percentiles = ParameterString(
    name="RFMPercentiles",
    default_value="20,40,60,80"
)

feature_group_name = ParameterString(
    name="FeatureGroupName",
    default_value="coca-cola-customer-features"
)

analysis_date = ParameterString(
    name="AnalysisDate",
    default_value=date_str
)

# Crear Processor para ejecutar el modelo
script_processor = ScriptProcessor(
    image_uri="REPLACE_WITH_CONTAINER_IMAGE_URI", # Imagen con pandas, scikit-learn, etc.
    command=["python3"],
    instance_type=processing_instance_type,
    instance_count=processing_instance_count,
    base_job_name=f"{base_job_prefix}-processor",
    role=role,
    sagemaker_session=sagemaker_session
)

# Paso de procesamiento para RFM
processing_step = ProcessingStep(
    name="ProcessRFMClustering",
    processor=script_processor,
    inputs=[
        ProcessingInput(
            source=input_data,
            destination="/opt/ml/processing/input",
        ),
    ],
    outputs=[
        ProcessingOutput(
            output_name="rfm_segments",
            source="/opt/ml/processing/output",
            destination=f"{model_output}/segments/{date_str}/"
        ),
        ProcessingOutput(
            output_name="model",
            source="/opt/ml/processing/model",
            destination=f"{model_output}/model/{date_str}/"
        ),
    ],
    code="REPLACE_WITH_S3_URI_TO_RFM_MODEL_SCRIPT", # Ruta al script dynamic-rfm-model.py
    job_arguments=[
        "--input-data", "/opt/ml/processing/input",
        "--output-data", "/opt/ml/processing/output/rfm_segments.parquet",
        "--model-dir", "/opt/ml/processing/model",
        "--n-clusters", n_clusters,
        "--k-percentiles", k_percentiles,
        "--analysis-date", analysis_date,
        "--feature-group-name", feature_group_name
    ],
)

# Archivo de propiedades para evaluar la calidad del modelo
evaluation_report = PropertyFile(
    name="EvaluationReport",
    output_name="model",
    path="evaluation.json"
)

# Configuración de calidad del modelo (definir umbrales basados en métricas relevantes)
# Este ejemplo comprueba que al menos el 90% de los clientes tengan segmento asignado
evaluation_step = ConditionStep(
    name="CheckModelQuality",
    conditions=[
        ConditionGreaterThanOrEqualTo(
            left=JsonGet(
                step_name=processing_step.name,
                property_file=evaluation_report,
                json_path="segment_coverage_percentage"
            ),
            right=90.0
        )
    ],
    if_steps=[],  # Si pasa el umbral, no hacemos nada adicional
    else_steps=[]  # Si no pasa, podríamos añadir pasos de notificación
)

# Definir pipeline
pipeline_name = f"{base_job_prefix}-Pipeline"
pipeline = Pipeline(
    name=pipeline_name,
    parameters=[
        processing_instance_type,
        processing_instance_count,
        input_data,
        model_output,
        n_clusters,
        k_percentiles,
        feature_group_name,
        analysis_date
    ],
    steps=[processing_step, evaluation_step],
    sagemaker_session=sagemaker_session,
)

# Definición para guardar el pipeline
pipeline_definition = json.loads(pipeline.definition())
print(pipeline.name)
pipeline.upsert(role_arn=role)

# Programar ejecución diaria con EventBridge
# Esto requiere crear una regla en EventBridge que ejecute el pipeline diariamente
cloudwatch_events = boto3.client('events')

rule_name = f"{base_job_prefix}-DailyExecution"

# Crear o actualizar regla (ejecución diaria a la 1 AM UTC)
response = cloudwatch_events.put_rule(
    Name=rule_name,
    ScheduleExpression="cron(0 1 * * ? *)",
    State="ENABLED"
)

# Configurar el destino para invocar el pipeline de SageMaker
pipeline_arn = pipeline.arn

response = cloudwatch_events.put_targets(
    Rule=rule_name,
    Targets=[
        {
            'Id': f"{base_job_prefix}-Target",
            'Arn': pipeline_arn,
            'RoleArn': "REPLACE_WITH_EVENTS_EXECUTION_ROLE_ARN",
            'Input': json.dumps({
                'PipelineParameters': {
                    'AnalysisDate': "${aws:CurrentDate}"
                }
            })
        }
    ]
)

print(f"Pipeline configurado con ejecución diaria programada: {rule_name}")

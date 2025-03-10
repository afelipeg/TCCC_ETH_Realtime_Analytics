AWSTemplateFormatVersion: '2010-09-09'
Description: 'Serverless Real-Time Analytics Platform for multiple data sources'

Parameters:
  ProjectName:
    Type: String
    Default: AnalyticsPlatform
    Description: Nombre del proyecto

  Environment:
    Type: String
    Default: dev
    AllowedValues:
      - dev
      - qa
      - prod
    Description: Entorno de despliegue

  KinesisShardCount:
    Type: Number
    Default: 1
    Description: Número de shards para Kinesis Data Stream

  S3BucketSuffix:
    Type: String
    Default: analytics-data
    Description: Sufijo para los buckets de S3

Resources:
  # ---------- IAM Roles ----------
  LambdaExecutionRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
        - arn:aws:iam::aws:policy/AmazonKinesisFullAccess
        - arn:aws:iam::aws:policy/AmazonS3FullAccess
        - arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess
        - arn:aws:iam::aws:policy/AmazonAthenaFullAccess
        - arn:aws:iam::aws:policy/AmazonGlueConsoleFullAccess

  FirehoseRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: firehose.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/AmazonKinesisFullAccess
        - arn:aws:iam::aws:policy/AmazonS3FullAccess
        - arn:aws:iam::aws:policy/AmazonLambdaExecute

  EventBridgeRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: events.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/AmazonLambdaExecute

  GlueServiceRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: glue.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
        - arn:aws:iam::aws:policy/AmazonS3FullAccess

  # ---------- S3 Buckets ----------
  RawDataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub ${AWS::AccountId}-${ProjectName}-raw-${S3BucketSuffix}-${Environment}
      VersioningConfiguration:
        Status: Enabled
      LifecycleConfiguration:
        Rules:
          - Id: TransitionToGlacierAfterOneYear
            Status: Enabled
            Transitions:
              - TransitionInDays: 365
                StorageClass: GLACIER
            ExpirationInDays: 1825

  CuratedDataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub ${AWS::AccountId}-${ProjectName}-curated-${S3BucketSuffix}-${Environment}
      VersioningConfiguration:
        Status: Enabled

  AthenaBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub ${AWS::AccountId}-${ProjectName}-athena-${S3BucketSuffix}-${Environment}

  # ---------- Kinesis Resources ----------
  AnalyticsDataStream:
    Type: AWS::Kinesis::Stream
    Properties:
      Name: !Sub ${ProjectName}-analytics-stream-${Environment}
      ShardCount: !Ref KinesisShardCount
      RetentionPeriodHours: 24
      StreamEncryption:
        EncryptionType: KMS
        KeyId: alias/aws/kinesis

  # ---------- Firehose Delivery Streams ----------
  RawDataFirehose:
    Type: AWS::KinesisFirehose::DeliveryStream
    DependsOn: RawDataBucket
    Properties:
      DeliveryStreamName: !Sub ${ProjectName}-raw-data-firehose-${Environment}
      DeliveryStreamType: KinesisStreamAsSource
      KinesisStreamSourceConfiguration:
        KinesisStreamARN: !GetAtt AnalyticsDataStream.Arn
        RoleARN: !GetAtt FirehoseRole.Arn
      ExtendedS3DestinationConfiguration:
        BucketARN: !GetAtt RawDataBucket.Arn
        RoleARN: !GetAtt FirehoseRole.Arn
        Prefix: "data/source=!{partitionKeyFromLambda:source}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
        ErrorOutputPrefix: "error/source=!{partitionKeyFromLambda:source}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/!{firehose:error-output-type}/"
        BufferingHints:
          IntervalInSeconds: 60
          SizeInMBs: 5
        CompressionFormat: GZIP
        ProcessingConfiguration:
          Enabled: true
          Processors:
            - Type: Lambda
              Parameters:
                - ParameterName: LambdaArn
                  ParameterValue: !GetAtt FirehoseProcessorLambda.Arn
                - ParameterName: NumberOfRetries
                  ParameterValue: "3"

  # ---------- DynamoDB Tables ----------
  MetadataTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Sub ${ProjectName}-metadata-${Environment}
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: dataSourceId
          AttributeType: S
        - AttributeName: timestamp
          AttributeType: S
      KeySchema:
        - AttributeName: dataSourceId
          KeyType: HASH
        - AttributeName: timestamp
          KeyType: RANGE
      StreamSpecification:
        StreamViewType: NEW_AND_OLD_IMAGES
      TimeToLiveSpecification:
        AttributeName: ttl
        Enabled: true

  # ---------- Lambda Functions ----------
  DataIngestionLambda:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub ${ProjectName}-data-ingestion-${Environment}
      Runtime: python3.11
      Handler: index.handler
      Role: !GetAtt LambdaExecutionRole.Arn
      Code:
        ZipFile: |
          import json
          import boto3
          import time
          import uuid
          import os

          kinesis = boto3.client('kinesis')
          stream_name = os.environ['KINESIS_STREAM_NAME']

          def handler(event, context):
              source = event.get('source', 'unknown')
              records = event.get('records', [])
              
              if not records:
                  return {
                      'statusCode': 400,
                      'body': json.dumps('No records provided')
                  }
              
              batch = []
              for record in records:
                  # Add source and timestamp to each record
                  if isinstance(record, dict):
                      record['source'] = source
                      record['ingest_timestamp'] = int(time.time() * 1000)
                      
                      # Put record to Kinesis
                      batch.append({
                          'Data': json.dumps(record).encode('utf-8'),
                          'PartitionKey': source
                      })
              
              # Send batch to Kinesis
              response = kinesis.put_records(
                  Records=batch,
                  StreamName=stream_name
              )
              
              return {
                  'statusCode': 200,
                  'body': json.dumps({
                      'SuccessfulCount': response.get('Records', []),
                      'FailedCount': response.get('FailedRecordCount', 0)
                  })
              }
      Environment:
        Variables:
          KINESIS_STREAM_NAME: !Ref AnalyticsDataStream
      Timeout: 60
      MemorySize: 256

  FirehoseProcessorLambda:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub ${ProjectName}-firehose-processor-${Environment}
      Runtime: python3.11
      Handler: index.handler
      Role: !GetAtt LambdaExecutionRole.Arn
      Code:
        ZipFile: |
          import json
          import base64
          from datetime import datetime

          def handler(event, context):
              output_records = []
              
              for record in event['records']:
                  data = base64.b64decode(record['data']).decode('utf-8')
                  
                  try:
                      # Parse JSON data
                      json_data = json.loads(data)
                      
                      # Get source data for partitioning
                      source = json_data.get('source', 'unknown')
                      
                      # Add metadata for Firehose dynamic partitioning
                      output_record = {
                          'recordId': record['recordId'],
                          'result': 'Ok',
                          'data': base64.b64encode(json.dumps(json_data).encode('utf-8')).decode('utf-8'),
                          'metadata': {
                              'partitionKeys': {
                                  'source': source
                              }
                          }
                      }
                  except Exception as e:
                      # Handle error
                      output_record = {
                          'recordId': record['recordId'],
                          'result': 'ProcessingFailed',
                          'data': record['data']
                      }
                      
                  output_records.append(output_record)
                  
              return {'records': output_records}
      Timeout: 60
      MemorySize: 256

  ETLLambda:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub ${ProjectName}-etl-${Environment}
      Runtime: python3.11
      Handler: index.handler
      Role: !GetAtt LambdaExecutionRole.Arn
      Code:
        ZipFile: |
          import json
          import boto3
          import os
          import re
          from datetime import datetime

          s3 = boto3.client('s3')
          glue = boto3.client('glue')
          
          RAW_BUCKET = os.environ['RAW_BUCKET']
          CURATED_BUCKET = os.environ['CURATED_BUCKET']
          GLUE_DATABASE = os.environ['GLUE_DATABASE']

          def handler(event, context):
              # Extract event data
              records = []
              for record in event.get('Records', []):
                  # Process S3 events
                  if 's3' in record.get('eventSource', ''):
                      bucket = record['s3']['bucket']['name']
                      key = record['s3']['object']['key']
                      
                      if bucket == RAW_BUCKET:
                          # Extraer la fuente de datos del path
                          source_match = re.search(r'source=([^/]+)', key)
                          source = source_match.group(1) if source_match else 'unknown'
                          
                          # Extraer la fecha del path
                          date_pattern = re.compile(r'year=(\d{4})/month=(\d{2})/day=(\d{2})')
                          date_match = date_pattern.search(key)
                          
                          date_str = None
                          if date_match:
                              year, month, day = date_match.groups()
                              date_str = f"{year}-{month}-{day}"
                          
                          # Construir el path de destino según la fuente y fecha
                          destination_path = f"data/processed/{source}/"
                          if date_str:
                              destination_path += f"dt={date_str}/"
                          
                          # Trigger Glue job for ETL
                          job_params = {
                              '--source_path': f"s3://{bucket}/{key}",
                              '--destination_path': f"s3://{CURATED_BUCKET}/{destination_path}",
                              '--database_name': GLUE_DATABASE,
                              '--source_name': source
                          }
                          
                          if date_str:
                              job_params['--data_date'] = date_str
                          
                          response = glue.start_job_run(
                              JobName=os.environ['GLUE_JOB_NAME'],
                              Arguments=job_params
                          )
                          
                          print(f"Iniciando Glue job para archivo s3://{bucket}/{key}")
                          print(f"JobRunId: {response['JobRunId']}")
                          print(f"Parámetros: {job_params}")
                          
                          return {
                              'statusCode': 200,
                              'body': json.dumps({
                                  'message': f"Started Glue job: {response['JobRunId']}",
                                  'source': source,
                                  'date': date_str,
                                  'jobRunId': response['JobRunId']
                              })
                          }
              
              return {
                  'statusCode': 200,
                  'body': json.dumps('Processed event')
              }
      Environment:
        Variables:
          RAW_BUCKET: !Ref RawDataBucket
          CURATED_BUCKET: !Ref CuratedDataBucket
          GLUE_DATABASE: !Ref GlueDatabase
          GLUE_JOB_NAME: !Ref ETLGlueJob
      Timeout: 60
      MemorySize: 256

  ReverseETLLambda:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub ${ProjectName}-reverse-etl-${Environment}
      Runtime: python3.11
      Handler: index.handler
      Role: !GetAtt LambdaExecutionRole.Arn
      Code:
        ZipFile: |
          import json
          import boto3
          import os
          import requests
          from datetime import datetime

          athena = boto3.client('athena')
          
          ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
          ATHENA_OUTPUT_LOCATION = os.environ['ATHENA_OUTPUT_LOCATION']
          
          def handler(event, context):
              # This function would execute SQL queries and send data back to source systems
              # Example: send aggregated data back to a marketing system
              
              # Run Athena query
              query = """
              SELECT source, COUNT(*) as count, AVG(value) as average
              FROM processed_data
              WHERE year = '2023' AND month = '01' AND day = '01'
              GROUP BY source
              """
              
              response = athena.start_query_execution(
                  QueryString=query,
                  QueryExecutionContext={
                      'Database': ATHENA_DATABASE
                  },
                  ResultConfiguration={
                      'OutputLocation': ATHENA_OUTPUT_LOCATION,
                  }
              )
              
              # In a real implementation, you would wait for the query to complete
              # and then send the results to the destination system
              # This is simplified for the template
              
              return {
                  'statusCode': 200,
                  'body': json.dumps('Reverse ETL process initiated')
              }
      Environment:
        Variables:
          ATHENA_DATABASE: !Ref AthenaDatabase
          ATHENA_OUTPUT_LOCATION: !Sub s3://${AthenaBucket}/query-results/
      Timeout: 300
      MemorySize: 512

  # ---------- API Gateway ----------
  AnalyticsAPI:
    Type: AWS::ApiGateway::RestApi
    Properties:
      Name: !Sub ${ProjectName}-api-${Environment}
      Description: API for data ingestion and analytics
      EndpointConfiguration:
        Types:
          - REGIONAL

  IngestionResource:
    Type: AWS::ApiGateway::Resource
    Properties:
      RestApiId: !Ref AnalyticsAPI
      ParentId: !GetAtt AnalyticsAPI.RootResourceId
      PathPart: ingest

  IngestionMethod:
    Type: AWS::ApiGateway::Method
    Properties:
      RestApiId: !Ref AnalyticsAPI
      ResourceId: !Ref IngestionResource
      HttpMethod: POST
      AuthorizationType: NONE
      Integration:
        Type: AWS
        IntegrationHttpMethod: POST
        Uri: !Sub arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${DataIngestionLambda.Arn}/invocations
        IntegrationResponses:
          - StatusCode: 200
        RequestTemplates:
          application/json: $input.body
      MethodResponses:
        - StatusCode: 200

  ApiGatewayDeployment:
    Type: AWS::ApiGateway::Deployment
    DependsOn: IngestionMethod
    Properties:
      RestApiId: !Ref AnalyticsAPI
      StageName: !Ref Environment

  # ---------- Lambda Permissions ----------
  DataIngestionLambdaPermission:
    Type: AWS::Lambda::Permission
    Properties:
      Action: lambda:InvokeFunction
      FunctionName: !Ref DataIngestionLambda
      Principal: apigateway.amazonaws.com
      SourceArn: !Sub arn:aws:execute-api:${AWS::Region}:${AWS::AccountId}:${AnalyticsAPI}/*

  FirehoseProcessorLambdaPermission:
    Type: AWS::Lambda::Permission
    Properties:
      Action: lambda:InvokeFunction
      FunctionName: !Ref FirehoseProcessorLambda
      Principal: firehose.amazonaws.com
      SourceArn: !GetAtt RawDataFirehose.Arn

  # ---------- Glue Resources ----------
  GlueDatabase:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput:
        Name: !Sub ${ProjectName}-analytics-db-${Environment}
        Description: Database for analytics data

  AthenaDatabase:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput:
        Name: !Sub ${ProjectName}-athena-db-${Environment}
        Description: Database for Athena queries

  RawDataCrawler:
    Type: AWS::Glue::Crawler
    Properties:
      Name: !Sub ${ProjectName}-raw-data-crawler-${Environment}
      Role: !GetAtt GlueServiceRole.Arn
      DatabaseName: !Ref GlueDatabase
      Targets:
        S3Targets:
          - Path: !Sub s3://${RawDataBucket}/data/
      Schedule:
        ScheduleExpression: cron(0 */6 * * ? *)
      SchemaChangePolicy:
        UpdateBehavior: UPDATE_IN_DATABASE
        DeleteBehavior: LOG
      Configuration: |
        {
          "Version": 1.0,
          "CrawlerOutput": {
            "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" }
          }
        }

  CuratedDataCrawler:
    Type: AWS::Glue::Crawler
    Properties:
      Name: !Sub ${ProjectName}-curated-data-crawler-${Environment}
      Role: !GetAtt GlueServiceRole.Arn
      DatabaseName: !Ref AthenaDatabase
      Targets:
        S3Targets:
          - Path: !Sub s3://${CuratedDataBucket}/
      Schedule:
        ScheduleExpression: cron(0 */6 * * ? *)
      SchemaChangePolicy:
        UpdateBehavior: UPDATE_IN_DATABASE
        DeleteBehavior: LOG
      Configuration: |
        {
          "Version": 1.0,
          "CrawlerOutput": {
            "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" }
          }
        }

  # Script para ETL de Glue
  ETLScriptDeployment:
    Type: AWS::Lambda::Function
    DependsOn: CuratedDataBucket
    Properties:
      FunctionName: !Sub ${ProjectName}-deploy-etl-script-${Environment}
      Runtime: python3.11
      Handler: index.handler
      Role: !GetAtt LambdaExecutionRole.Arn
      Code:
        ZipFile: |
          import boto3
          import os
          import cfnresponse
          
          s3 = boto3.client('s3')
          
          def create_etl_script():
              # Contenido del script de ETL para Glue
              return '''
              import sys
              from awsglue.transforms import *
              from awsglue.utils import getResolvedOptions
              from pyspark.context import SparkContext
              from awsglue.context import GlueContext
              from awsglue.job import Job
              from awsglue.dynamicframe import DynamicFrame
              from pyspark.sql.functions import col, from_json, to_date, year, month, day, hour, current_timestamp
              from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
              
              # Inicialización de Glue
              args = getResolvedOptions(sys.argv, [
                  'JOB_NAME', 
                  'source_path', 
                  'destination_path', 
                  'database_name',
                  'source_name',
                  'data_date'
              ])
              
              sc = SparkContext()
              glueContext = GlueContext(sc)
              spark = glueContext.spark_session
              job = Job(glueContext)
              job.init(args['JOB_NAME'], args)
              
              # Obtener parámetros
              source_path = args['source_path']
              destination_path = args['destination_path']
              database_name = args['database_name']
              source_name = args['source_name']
              data_date = args['data_date']
              
              print(f"Procesando datos de {source_name} para fecha {data_date}")
              print(f"Leyendo desde: {source_path}")
              print(f"Escribiendo hacia: {destination_path}")
              
              # Configuración específica según la fuente
              source_config = {
                  'magento': {
                      'schema': {
                          'order_id': 'string',
                          'customer_id': 'string',
                          'order_date': 'timestamp',
                          'total_amount': 'double',
                          'status': 'string',
                          'items': 'array<struct<product_id:string,quantity:int,price:double>>'
                      },
                      'partition_cols': ['year', 'month', 'day']
                  },
                  'aem': {
                      'schema': {
                          'asset_id': 'string',
                          'asset_type': 'string',
                          'path': 'string',
                          'size': 'int',
                          'created_date': 'timestamp',
                          'modified_date': 'timestamp',
                          'metadata': 'map<string,string>'
                      },
                      'partition_cols': ['year', 'month', 'day', 'asset_type']
                  },
                  'google_analytics': {
                      'schema': {
                          'session_id': 'string',
                          'user_id': 'string',
                          'event_timestamp': 'timestamp',
                          'page_path': 'string',
                          'event_name': 'string',
                          'device_category': 'string',
                          'country': 'string',
                          'metrics': 'map<string,double>'
                      },
                      'partition_cols': ['year', 'month', 'day', 'event_name']
                  },
                  'fanplayr': {
                      'schema': {
                          'interaction_id': 'string',
                          'session_id': 'string',
                          'user_id': 'string',
                          'timestamp': 'timestamp',
                          'action': 'string',
                          'offer_id': 'string',
                          'response': 'string',
                          'revenue_impact': 'double'
                      },
                      'partition_cols': ['year', 'month', 'day', 'action']
                  },
                  'mysql': {
                      'schema': {
                          'id': 'string',
                          'table_name': 'string',
                          'operation': 'string',
                          'timestamp': 'timestamp',
                          'data': 'string'
                      },
                      'partition_cols': ['year', 'month', 'day', 'table_name']
                  }
              }
              
              # Obtener configuración para la fuente actual o usar configuración genérica
              config = source_config.get(source_name, {
                  'schema': {},
                  'partition_cols': ['year', 'month', 'day']
              })
              
              # Leer los datos en formato JSON
              input_dyf = glueContext.create_dynamic_frame.from_options(
                  connection_type="s3",
                  connection_options={"paths": [source_path]},
                  format="json"
              )
              
              print(f"Schema original: {input_dyf.schema()}")
              
              # Convertir a DataFrame para procesamiento más avanzado
              input_df = input_dyf.toDF()
              
              # Adicionar columnas de partición basadas en la fecha
              if 'timestamp' in input_df.columns:
                  date_col = 'timestamp'
              elif 'event_timestamp' in input_df.columns:
                  date_col = 'event_timestamp'
              elif 'order_date' in input_df.columns:
                  date_col = 'order_date'
              elif 'created_date' in input_df.columns:
                  date_col = 'created_date'
              else:
                  # Si no hay columna de fecha, usamos la fecha del archivo
                  input_df = input_df.withColumn('processing_date', to_date(lit(data_date), 'yyyy-MM-dd'))
                  date_col = 'processing_date'
              
              # Agregar columnas de particionamiento
              df_with_partitions = input_df \
                  .withColumn('year', year(col(date_col))) \
                  .withColumn('month', month(col(date_col))) \
                  .withColumn('day', day(col(date_col))) \
                  .withColumn('processing_timestamp', current_timestamp())
              
              # Convertir de vuelta a DynamicFrame
              output_dyf = DynamicFrame.fromDF(df_with_partitions, glueContext, "output_dyf")
              
              # Escribir datos procesados
              sink = glueContext.write_dynamic_frame.from_options(
                  frame=output_dyf,
                  connection_type="s3",
                  connection_options={
                      "path": destination_path,
                      "partitionKeys": config['partition_cols']
                  },
                  format="parquet"
              )
              
              # Actualizar el catálogo de datos
              glueContext.create_dynamic_frame.from_catalog(
                  database=database_name,
                  table_name=f"{source_name}_processed"
              )
              
              job.commit()
              '''
          
          def write_file_to_s3(bucket, key, content):
              s3.put_object(
                  Bucket=bucket,
                  Key=key,
                  Body=content
              )
              return f"s3://{bucket}/{key}"
          
          def handler(event, context):
              try:
                  request_type = event['RequestType']
                  bucket = event['ResourceProperties']['BucketName']
                  script_key = "scripts/etl_job.py"
                  
                  if request_type == 'Create' or request_type == 'Update':
                      # Crear el script de ETL
                      etl_script = create_etl_script()
                      script_location = write_file_to_s3(bucket, script_key, etl_script)
                      
                      responseData = {
                          'ScriptLocation': script_location
                      }
                      cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
                  elif request_type == 'Delete':
                      # No hacemos nada en caso de eliminación
                      cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
                  else:
                      cfnresponse.send(event, context, cfnresponse.FAILED, {})
              except Exception as e:
                  print(f"Error: {str(e)}")
                  cfnresponse.send(event, context, cfnresponse.FAILED, {})
      Timeout: 60
      MemorySize: 256

  # Desplegar script de ETL
  DeployETLScript:
    Type: AWS::CloudFormation::CustomResource
    DependsOn: ETLScriptDeployment
    Properties:
      ServiceToken: !GetAtt ETLScriptDeployment.Arn
      BucketName: !Ref CuratedDataBucket

  ETLGlueJob:
    Type: AWS::Glue::Job
    DependsOn: DeployETLScript
    Properties:
      Name: !Sub ${ProjectName}-etl-job-${Environment}
      Role: !GetAtt GlueServiceRole.Arn
      Command:
        Name: glueetl
        ScriptLocation: !Sub s3://${CuratedDataBucket}/scripts/etl_job.py
        PythonVersion: '3'
      DefaultArguments:
        '--job-language': 'python'
        '--enable-metrics': ''
        '--enable-continuous-cloudwatch-log': 'true'
        '--TempDir': !Sub s3://${CuratedDataBucket}/temp/
        '--enable-spark-ui': 'true'
        '--enable-job-insights': 'true'
        '--source_name': 'default'
        '--data_date': '2023-01-01'
      MaxRetries: 2
      Timeout: 60
      GlueVersion: '3.0'
      NumberOfWorkers: 5
      WorkerType: G.1X

  # ---------- EventBridge Rules ----------
  S3NotificationRule:
    Type: AWS::Events::Rule
    Properties:
      Name: !Sub ${ProjectName}-s3-notification-rule-${Environment}
      Description: Trigger ETL process when new data arrives in raw bucket
      EventPattern:
        source:
          - aws.s3
        detail-type:
          - Object Created
        detail:
          bucket:
            name:
              - !Ref RawDataBucket
      State: ENABLED
      Targets:
        - Arn: !GetAtt ETLLambda.Arn
          Id: ETLTarget
          RoleArn: !GetAtt EventBridgeRole.Arn

  ETLLambdaPermissionForEventBridge:
    Type: AWS::Lambda::Permission
    Properties:
      Action: lambda:InvokeFunction
      FunctionName: !Ref ETLLambda
      Principal: events.amazonaws.com
      SourceArn: !GetAtt S3NotificationRule.Arn

  # ---------- Scheduled Events ----------
  DailyDataIngestionRule:
    Type: AWS::Events::Rule
    Properties:
      Name: !Sub ${ProjectName}-daily-ingestion-rule-${Environment}
      Description: Trigger data ingestion process daily
      ScheduleExpression: cron(0 1 * * ? *) # Ejecuta diariamente a la 1 AM UTC
      State: ENABLED
      Targets:
        - Arn: !GetAtt ScheduledIngestionLambda.Arn
          Id: ScheduledIngestionTarget
          RoleArn: !GetAtt EventBridgeRole.Arn

  ScheduledIngestionLambda:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub ${ProjectName}-scheduled-ingestion-${Environment}
      Runtime: python3.11
      Handler: index.handler
      Role: !GetAtt LambdaExecutionRole.Arn
      Code:
        ZipFile: |
          import json
          import boto3
          import os
          import requests
          from datetime import datetime, timedelta

          lambda_client = boto3.client('lambda')
          
          # Configuración de fuentes de datos
          DATA_SOURCES = [
            {
              'name': 'magento',
              'type': 'api',
              'config': {
                'endpoint': 'https://{{YOUR_MAGENTO_INSTANCE}}/api/analytics/export',
                'auth_type': 'oauth'
              }
            },
            {
              'name': 'aem',
              'type': 'api',
              'config': {
                'endpoint': 'https://{{YOUR_AEM_INSTANCE}}/content/dam/analytics/export'
              }
            },
            {
              'name': 'google_analytics',
              'type': 'api',
              'config': {
                'endpoint': 'https://analyticsdata.googleapis.com/v1beta'
              }
            },
            {
              'name': 'fanplayr',
              'type': 'api',
              'config': {
                'endpoint': 'https://api.fanplayr.com/v1/data'
              }
            },
            {
              'name': 'mysql',
              'type': 'database',
              'config': {
                'host': '{{YOUR_MYSQL_HOST}}',
                'port': 3306,
                'tables': ['customers', 'orders', 'products']
              }
            }
          ]
          
          def handler(event, context):
              # Fecha para la que se obtendrán los datos (ayer)
              yesterday = datetime.now() - timedelta(days=1)
              date_str = yesterday.strftime('%Y-%m-%d')
              
              results = []
              
              for source in DATA_SOURCES:
                  try:
                      print(f"Iniciando ingesta para {source['name']} para fecha {date_str}")
                      
                      # Aquí implementaría la lógica de obtención de datos según el tipo de fuente
                      # En un entorno real, esto requeriría autenticación y configuración específica
                      
                      # Simulación de obtención de datos
                      data = {
                          'source': source['name'],
                          'date': date_str,
                          'records': [{'sample': 'data'}]  # Esto sería reemplazado por datos reales
                      }
                      
                      # Invocar la lambda de ingesta para procesar los datos
                      response = lambda_client.invoke(
                          FunctionName=os.environ['INGESTION_LAMBDA'],
                          InvocationType='RequestResponse',
                          Payload=json.dumps(data)
                      )
                      
                      payload = json.loads(response['Payload'].read())
                      results.append({
                          'source': source['name'],
                          'status': 'success',
                          'details': payload
                      })
                      
                  except Exception as e:
                      print(f"Error procesando fuente {source['name']}: {str(e)}")
                      results.append({
                          'source': source['name'],
                          'status': 'error',
                          'error': str(e)
                      })
              
              return {
                  'statusCode': 200,
                  'body': json.dumps({
                      'timestamp': datetime.now().isoformat(),
                      'ingestDate': date_str,
                      'results': results
                  })
              }
      Environment:
        Variables:
          INGESTION_LAMBDA: !Ref DataIngestionLambda
      Timeout: 900  # 15 minutos para procesar todas las fuentes
      MemorySize: 512

  ScheduledIngestionLambdaPermission:
    Type: AWS::Lambda::Permission
    Properties:
      Action: lambda:InvokeFunction
      FunctionName: !Ref ScheduledIngestionLambda
      Principal: events.amazonaws.com
      SourceArn: !GetAtt DailyDataIngestionRule.Arn

  AthenaQueryLambda:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub ${ProjectName}-athena-query-${Environment}
      Runtime: python3.11
      Handler: index.handler
      Role: !GetAtt LambdaExecutionRole.Arn
      Code:
        ZipFile: |
          import json
          import boto3
          import time
          import os
          from datetime import datetime

          athena = boto3.client('athena')
          s3 = boto3.client('s3')
          
          ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
          ATHENA_OUTPUT_LOCATION = os.environ['ATHENA_OUTPUT_LOCATION']
          
          # Consultas predefinidas para cada vista de negocio
          PREDEFINED_QUERIES = {
              'daily_sales': """
                  SELECT 
                      date_trunc('day', order_date) as day,
                      COUNT(*) as order_count,
                      SUM(total_amount) as total_sales,
                      AVG(total_amount) as avg_order_value
                  FROM orders
                  WHERE order_date BETWEEN DATE_ADD('day', -30, CURRENT_DATE) AND CURRENT_DATE
                  GROUP BY date_trunc('day', order_date)
                  ORDER BY day DESC
              """,
              'product_performance': """
                  SELECT 
                      p.product_id,
                      p.name,
                      p.category,
                      COUNT(oi.order_id) as times_ordered,
                      SUM(oi.quantity) as units_sold,
                      SUM(oi.quantity * oi.price) as revenue
                  FROM products p
                  JOIN order_items oi ON p.product_id = oi.product_id
                  JOIN orders o ON oi.order_id = o.order_id
                  WHERE o.order_date BETWEEN DATE_ADD('day', -30, CURRENT_DATE) AND CURRENT_DATE
                  GROUP BY p.product_id, p.name, p.category
                  ORDER BY revenue DESC
                  LIMIT 100
              """,
              'customer_segments': """
                  SELECT 
                      customer_segment,
                      COUNT(*) as customer_count,
                      SUM(lifetime_value) as total_value,
                      AVG(lifetime_value) as avg_value
                  FROM customer_profiles
                  GROUP BY customer_segment
                  ORDER BY total_value DESC
              """
          }
          
          def wait_for_query_completion(query_execution_id):
              """Espera a que se complete una consulta de Athena"""
              while True:
                  response = athena.get_query_execution(QueryExecutionId=query_execution_id)
                  state = response['QueryExecution']['Status']['State']
                  
                  if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                      return state
                      
                  # Sleep para no sobrecargar la API
                  time.sleep(1)
          
          def get_query_results(query_execution_id):
              """Obtiene los resultados de una consulta completada"""
              response = athena.get_query_results(QueryExecutionId=query_execution_id)
              return response
          
          def handler(event, context):
              # Ejecutar reportes predefinidos
              results = {}
              
              for query_name, query_sql in PREDEFINED_QUERIES.items():
                  try:
                      # Ejecutar consulta
                      response = athena.start_query_execution(
                          QueryString=query_sql,
                          QueryExecutionContext={
                              'Database': ATHENA_DATABASE
                          },
                          ResultConfiguration={
                              'OutputLocation': f"{ATHENA_OUTPUT_LOCATION}{query_name}/",
                          }
                      )
                      
                      query_execution_id = response['QueryExecutionId']
                      
                      # Esperar a que se complete la consulta
                      state = wait_for_query_completion(query_execution_id)
                      
                      if state == 'SUCCEEDED':
                          results[query_name] = {
                              'status': 'success',
                              'execution_id': query_execution_id,
                              'location': f"{ATHENA_OUTPUT_LOCATION}{query_name}/{query_execution_id}.csv"
                          }
                      else:
                          # Obtener detalles del error
                          error_details = athena.get_query_execution(QueryExecutionId=query_execution_id)
                          results[query_name] = {
                              'status': 'failed',
                              'execution_id': query_execution_id,
                              'error': error_details['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                          }
                  except Exception as e:
                      results[query_name] = {
                          'status': 'error',
                          'error': str(e)
                      }
              
              return {
                  'statusCode': 200,
                  'body': json.dumps({
                      'timestamp': datetime.now().isoformat(),
                      'results': results
                  })
              }
      Environment:
        Variables:
          ATHENA_DATABASE: !Ref AthenaDatabase
          ATHENA_OUTPUT_LOCATION: !Sub s3://${AthenaBucket}/reports/
      Timeout: 900  # 15 minutos
      MemorySize: 512

  # Regla para ejecutar reportes diarios
  DailyReportingRule:
    Type: AWS::Events::Rule
    Properties:
      Name: !Sub ${ProjectName}-daily-reporting-rule-${Environment}
      Description: Generate daily reports using Athena
      ScheduleExpression: cron(0 5 * * ? *) # Ejecuta diariamente a las 5 AM UTC
      State: ENABLED
      Targets:
        - Arn: !GetAtt AthenaQueryLambda.Arn
          Id: AthenaQueryTarget
          RoleArn: !GetAtt EventBridgeRole.Arn

  AthenaQueryLambdaPermission:
    Type: AWS::Lambda::Permission
    Properties:
      Action: lambda:InvokeFunction
      FunctionName: !Ref AthenaQueryLambda
      Principal: events.amazonaws.com
      SourceArn: !GetAtt DailyReportingRule.Arn

Outputs:
  ApiEndpoint:
    Description: API Gateway endpoint URL for data ingestion
    Value: !Sub https://${AnalyticsAPI}.execute-api.${AWS::Region}.amazonaws.com/${Environment}/ingest

  KinesisDataStreamName:
    Description: Name of the Kinesis Data Stream
    Value: !Ref AnalyticsDataStream

  RawDataBucketName:
    Description: Name of the raw data bucket
    Value: !Ref RawDataBucket

  CuratedDataBucketName:
    Description: Name of the curated data bucket
    Value: !Ref CuratedDataBucket

  AthenaBucketName:
    Description: Name of the Athena query results bucket
    Value: !Ref AthenaBucket

  GlueDatabaseName:
    Description: Name of the Glue database
    Value: !Ref GlueDatabase

  AthenaDatabaseName:
    Description: Name of the Athena database
    Value: !Ref AthenaDatabase
    
  DailyIngestionSchedule:
    Description: Horario de ingesta programada diaria
    Value: "1:00 AM UTC"
    
  DailyReportingSchedule:
    Description: Horario de generación de reportes diarios
    Value: "5:00 AM UTC"
    
  DocumentationURL:
    Description: URL de documentación de la plataforma
    Value: "https://github.com/afelipeg/serverless-realtime-analytics"

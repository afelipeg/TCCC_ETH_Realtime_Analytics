import json
import boto3
import os
import requests
import pandas as pd
from io import BytesIO
import time
from datetime import datetime

# Configuración de AWS
athena = boto3.client('athena')
s3 = boto3.client('s3')

# Configuración desde variables de entorno
AEM_API_ENDPOINT = os.environ.get('AEM_API_ENDPOINT')
AEM_USERNAME = os.environ.get('AEM_USERNAME')
AEM_PASSWORD = os.environ.get('AEM_PASSWORD')
ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE')
ATHENA_OUTPUT_LOCATION = os.environ.get('ATHENA_OUTPUT_LOCATION')
ATHENA_CATALOG = os.environ.get('ATHENA_CATALOG', 'AwsDataCatalog')
MAX_BATCH_SIZE = int(os.environ.get('MAX_BATCH_SIZE', '100'))

def run_athena_query(query):
    """Ejecuta una consulta en Athena y retorna el resultado"""
    query_response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': ATHENA_DATABASE,
            'Catalog': ATHENA_CATALOG
        },
        ResultConfiguration={
            'OutputLocation': ATHENA_OUTPUT_LOCATION,
        }
    )
    
    query_execution_id = query_response['QueryExecutionId']
    
    # Esperar a que se complete la consulta
    state = 'RUNNING'
    while state in ['QUEUED', 'RUNNING']:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        
        if state in ['FAILED', 'CANCELLED']:
            error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            raise Exception(f"Query {query_execution_id} failed: {error_message}")
            
        if state != 'SUCCEEDED':
            time.sleep(1)  # Pausar 1 segundo antes de verificar de nuevo
    
    # Obtener ubicación del resultado
    result_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
    
    # Extraer bucket y key del resultado
    result_path = result_location.replace('s3://', '')
    bucket = result_path.split('/', 1)[0]
    key = result_path.split('/', 1)[1]
    
    # Descargar resultado
    result_obj = s3.get_object(Bucket=bucket, Key=key)
    result_data = result_obj['Body'].read()
    
    # Convertir a DataFrame
    return pd.read_csv(BytesIO(result_data))

def authenticate_aem():
    """Autentica con AEM y retorna el token de acceso"""
    auth_url = f"{AEM_API_ENDPOINT}/libs/granite/core/content/login.html/j_security_check"
    
    payload = {
        "j_username": AEM_USERNAME,
        "j_password": AEM_PASSWORD,
        "j_validate": "true"
    }
    
    session = requests.Session()
    response = session.post(auth_url, data=payload)
    
    if response.status_code != 200 or "Invalid credentials" in response.text:
        raise Exception("Failed to authenticate with AEM")
    
    return session

def activate_segments_in_aem(session, segments_df):
    """Envía los segmentos a AEM para activación de campañas"""
    activation_url = f"{AEM_API_ENDPOINT}/bin/coca-cola/segments/activate"
    
    # Procesar por lotes para evitar sobrecarga
    total_records = len(segments_df)
    total_batches = (total_records + MAX_BATCH_SIZE - 1) // MAX_BATCH_SIZE
    
    success_count = 0
    error_count = 0
    errors = []
    
    for batch_num in range(total_batches):
        start_idx = batch_num * MAX_BATCH_SIZE
        end_idx = min(start_idx + MAX_BATCH_SIZE, total_records)
        
        batch_df = segments_df.iloc[start_idx:end_idx]
        batch_records = batch_df.to_dict('records')
        
        payload = {
            "segments": batch_records,
            "activation_date": datetime.now().strftime("%Y-%m-%d"),
            "source": "aws_analytics_platform"
        }
        
        try:
            response = session.post(
                activation_url, 
                json=payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                result = response.json()
                success_count += result.get('success_count', 0)
                error_count += result.get('error_count', 0)
                
                if 'errors' in result and result['errors']:
                    errors.extend(result['errors'])
            else:
                error_count += len(batch_records)
                errors.append(f"Batch {batch_num+1} failed with status code: {response.status_code}")
                
        except Exception as e:
            error_count += len(batch_records)
            errors.append(f"Batch {batch_num+1} failed with error: {str(e)}")
        
        # Pequeña pausa entre lotes para no sobrecargar la API
        if batch_num < total_batches - 1:
            time.sleep(0.5)
    
    return {
        "total_records": total_records,
        "success_count": success_count,
        "error_count": error_count,
        "errors": errors[:10]  # Limitar número de errores en la respuesta
    }

def handler(event, context):
    """Función principal Lambda para activación de segmentos"""
    try:
        # Recuperar fecha de procesamiento (hoy por defecto)
        processing_date = event.get('processing_date', datetime.now().strftime("%Y-%m-%d"))
        segments = event.get('segments', [])  # Segmentos específicos a activar
        
        print(f"Iniciando activación de segmentos para fecha: {processing_date}")
        
        # Construir consulta Athena
        where_clause = ""
        if segments and len(segments) > 0:
            segments_str = "', '".join(segments)
            where_clause = f"AND segment IN ('{segments_str}')"
        
        query = f"""
        SELECT 
            customer_id,
            email,
            firstname,
            lastname,
            segment,
            r_score,
            f_score,
            m_score,
            recency_days,
            frequency,
            average_order_value,
            total_spent,
            city,
            region,
            country_id
        FROM 
            coca_cola_aem_activation
        WHERE 
            processing_date = '{processing_date}'
            {where_clause}
        """
        
        # Ejecutar consulta
        print("Ejecutando consulta en Athena...")
        segments_df = run_athena_query(query)
        
        if segments_df.empty:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No segments to activate',
                    'segments_count': 0
                })
            }
        
        print(f"Recuperados {len(segments_df)} registros de segmentos")
        
        # Autenticar con AEM
        print("Autenticando con AEM...")
        aem_session = authenticate_aem()
        
        # Activar segmentos
        print("Activando segmentos en AEM...")
        activation_result = activate_segments_in_aem(aem_session, segments_df)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Segment activation completed',
                'segments_count': len(segments_df),
                'processing_date': processing_date,
                'activation_results': activation_result
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error activating segments',
                'error': str(e)
            })
        }

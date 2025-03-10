import os
import json
import boto3
import numpy as np
import pandas as pd
import pickle
from io import BytesIO
import logging

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicializar clientes
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sagemaker_runtime = boto3.client('sagemaker-runtime')
featurestore_runtime = boto3.client('sagemaker-featurestore-runtime')

# Configuración desde variables de entorno
FEATURE_GROUP_NAME = os.environ.get('FEATURE_GROUP_NAME', 'coca-cola-customer-features')
MODEL_BUCKET = os.environ.get('MODEL_BUCKET')
CONFIG_KEY = os.environ.get('CONFIG_KEY')
MODEL_ENDPOINT = os.environ.get('MODEL_ENDPOINT')
CUSTOMER_TABLE = os.environ.get('CUSTOMER_TABLE', 'coca-cola-customer-segments')

def get_latest_config():
    """Obtiene la configuración más reciente del modelo"""
    try:
        # Listar las configuraciones disponibles
        response = s3_client.list_objects_v2(
            Bucket=MODEL_BUCKET,
            Prefix='rfm-models/config_'
        )
        
        if 'Contents' not in response:
            raise Exception("No se encontraron configuraciones")
        
        # Ordenar por fecha de modificación (más reciente primero)
        configs = sorted(
            response['Contents'],
            key=lambda x: x['LastModified'],
            reverse=True
        )
        
        # Obtener la configuración más reciente
        latest_config = configs[0]
        
        # Descargar y parsear la configuración
        config_obj = s3_client.get_object(
            Bucket=MODEL_BUCKET,
            Key=latest_config['Key']
        )
        
        config = json.loads(config_obj['Body'].read().decode('utf-8'))
        logger.info(f"Configuración cargada: {config}")
        
        return config
    
    except Exception as e:
        logger.error(f"Error obteniendo configuración: {str(e)}")
        # Usar una configuración por defecto
        return {
            'model_path': f"s3://{MODEL_BUCKET}/rfm-models/rfm_kmeans_model_latest.pkl",
            'k_percentiles': [20, 40, 60, 80],
            'n_clusters': 5
        }

def load_model_from_s3(model_path):
    """Carga el modelo de clustering desde S3"""
    try:
        bucket, key = model_path.replace("s3://", "").split("/", 1)
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        model_bytes = response['Body'].read()
        
        return pickle.loads(model_bytes)
    
    except Exception as e:
        logger.error(f"Error cargando modelo: {str(e)}")
        return None

def get_customer_data_from_feature_store(customer_id):
    """Obtiene los datos más recientes de un cliente desde Feature Store"""
    try:
        response = featurestore_runtime.get_record(
            FeatureGroupName=FEATURE_GROUP_NAME,
            RecordIdentifierValueAsString=str(customer_id)
        )
        
        # Convertir respuesta a diccionario
        customer_data = {}
        for feature in response['Record']:
            name = feature['FeatureName']
            value = feature['ValueAsString']
            
            # Intentar convertir a tipos adecuados
            if name in ['recency_days', 'frequency', 'monetary']:
                customer_data[name] = float(value)
            elif name in ['r_score', 'f_score', 'm_score', 'rfm_score', 'cluster_id']:
                customer_data[name] = int(value)
            else:
                customer_data[name] = value
                
        return customer_data, True
    
    except Exception as e:
        logger.warning(f"Cliente {customer_id} no encontrado en Feature Store: {str(e)}")
        return None, False

def calculate_rfm_scores(customer_data, k_percentiles, reference_data=None):
    """
    Calcula las puntuaciones RFM para un cliente individual
    
    Args:
        customer_data: Diccionario con datos del cliente
        k_percentiles: Lista de percentiles para la puntuación
        reference_data: Datos de referencia para calcular percentiles (opcional)
    
    Returns:
        Diccionario con puntuaciones R, F, M y RFM total
    """
    # Si tenemos datos de referencia, usarlos para calcular percentiles
    if reference_data is not None:
        # Calcular puntuación R (menor es mejor)
        r_bins = [0] + [np.percentile(reference_data['recency_days'], p) for p in k_percentiles]
        for i, (lower, upper) in enumerate(zip(r_bins[:-1], r_bins[1:])):
            if lower <= customer_data['recency_days'] <= upper:
                r_score = len(k_percentiles) - i
                break
        else:
            r_score = 1  # Default si no cae en ningún bin
            
        # Calcular puntuación F (mayor es mejor)
        f_bins = [0] + [np.percentile(reference_data['frequency'], p) for p in k_percentiles]
        for i, (lower, upper) in enumerate(zip(f_bins[:-1], f_bins[1:])):
            if lower <= customer_data['frequency'] <= upper:
                f_score = i + 1
                break
        else:
            f_score = len(k_percentiles)  # Default si no cae en ningún bin
            
        # Calcular puntuación M (mayor es mejor)
        m_bins = [0] + [np.percentile(reference_data['monetary'], p) for p in k_percentiles]
        for i, (lower, upper) in enumerate(zip(m_bins[:-1], m_bins[1:])):
            if lower <= customer_data['monetary'] <= upper:
                m_score = i + 1
                break
        else:
            m_score = len(k_percentiles)  # Default si no cae en ningún bin
    
    else:
        # Usar puntuaciones existentes si están disponibles
        r_score = customer_data.get('r_score', 3)  # Valores por defecto
        f_score = customer_data.get('f_score', 3)
        m_score = customer_data.get('m_score', 3)
    
    # Calcular puntuación RFM total
    rfm_score = r_score + f_score + m_score
    
    return {
        'r_score': r_score,
        'f_score': f_score,
        'm_score': m_score,
        'rfm_score': rfm_score
    }

def assign_segment(rfm_scores):
    """
    Asigna un segmento basado en puntuaciones RFM
    
    Args:
        rfm_scores: Diccionario con puntuaciones RFM
    
    Returns:
        Nombre del segmento asignado
    """
    r = rfm_scores['r_score']
    f = rfm_scores['f_score']
    m = rfm_scores['m_score']
    rfm = rfm_scores['rfm_score']
    
    # Lógica de asignación de segmentos
    if rfm >= 13:
        return 'Champions'
    elif rfm >= 11:
        return 'Loyal Customers'
    elif rfm >= 9:
        return 'Potential Loyalists'
    elif r >= 4 and rfm >= 7:
        return 'Promising'
    elif rfm >= 5:
        return 'Needs Attention'
    elif rfm >= 3:
        return 'At Risk'
    elif m >= 4:
        return "Can't Lose"
    else:
        return 'Lost'

def predict_segment_sagemaker(customer_data):
    """
    Realiza inferencia usando el endpoint de SageMaker
    
    Args:
        customer_data: Diccionario con datos del cliente
    
    Returns:
        Resultado de la predicción
    """
    try:
        # Preparar datos para la inferencia
        features = ['recency_days', 'frequency', 'monetary']
        payload = {feature: customer_data.get(feature, 0) for feature in features}
        
        # Invocar endpoint
        response = sagemaker_runtime.invoke_endpoint(
            EndpointName=MODEL_ENDPOINT,
            ContentType='application/json',
            Body=json.dumps(payload)
        )
        
        # Parsear respuesta
        result = json.loads(response['Body'].read().decode())
        return result
    
    except Exception as e:
        logger.error(f"Error invocando endpoint SageMaker: {str(e)}")
        return None

def update_dynamodb(customer_id, segment_data):
    """
    Actualiza la tabla de DynamoDB con el segmento del cliente
    
    Args:
        customer_id: ID del cliente
        segment_data: Datos del segmento
    
    Returns:
        True si la actualización fue exitosa, False en caso contrario
    """
    try:
        table = dynamodb.Table(CUSTOMER_TABLE)
        
        response = table.update_item(
            Key={'customer_id': str(customer_id)},
            UpdateExpression="set segment = :s, r_score = :r, f_score = :f, m_score = :m
import argparse
import os
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from datetime import datetime
import boto3
import json
import logging
import pickle
from io import StringIO

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def parse_args():
    """Parse arguments for processing job"""
    parser = argparse.ArgumentParser()
    # Data paths
    parser.add_argument('--input-data', type=str, required=True)
    parser.add_argument('--output-data', type=str, required=True)
    parser.add_argument('--model-dir', type=str, required=True)
    
    # Parámetros del modelo
    parser.add_argument('--n-clusters', type=int, default=5)
    parser.add_argument('--k-percentiles', type=str, default="20,40,60,80")
    parser.add_argument('--analysis-date', type=str)
    
    # Parámetros de Feature Store
    parser.add_argument('--feature-group-name', type=str, default="coca-cola-customer-features")
    
    return parser.parse_args()

def read_from_s3(s3_uri):
    """Read data from S3 and return as pandas DataFrame"""
    s3 = boto3.client('s3')
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(obj['Body'])

def write_to_s3(df, s3_uri, format='parquet'):
    """Write DataFrame to S3"""
    s3 = boto3.client('s3')
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    
    if format == 'parquet':
        buffer = StringIO()
        df.to_parquet(buffer)
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    elif format == 'csv':
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    elif format == 'json':
        s3.put_object(
            Bucket=bucket, 
            Key=key, 
            Body=df.to_json(orient='records', lines=True)
        )

def save_model(model, model_dir, filename):
    """Save model to specified directory"""
    s3 = boto3.client('s3')
    bucket, key_prefix = model_dir.replace("s3://", "").split("/", 1)
    
    # Guardar modelo con pickle
    model_bytes = pickle.dumps(model)
    key = f"{key_prefix}/{filename}"
    s3.put_object(Bucket=bucket, Key=key, Body=model_bytes)
    
    logger.info(f"Modelo guardado en s3://{bucket}/{key}")
    return f"s3://{bucket}/{key}"

def load_model(model_path):
    """Load model from specified path"""
    s3 = boto3.client('s3')
    bucket, key = model_path.replace("s3://", "").split("/", 1)
    
    response = s3.get_object(Bucket=bucket, Key=key)
    model_bytes = response['Body'].read()
    
    return pickle.loads(model_bytes)

def update_feature_store(df, feature_group_name):
    """Update SageMaker Feature Store with latest RFM features"""
    try:
        # Preparar los datos para Feature Store
        df['event_time'] = datetime.now().timestamp()
        
        # Asegurar que customer_id sea string para feature store
        df['customer_id'] = df['customer_id'].astype(str)
        
        # Transformar los datos al formato que Feature Store espera
        feature_records = []
        for _, row in df.iterrows():
            record = {
                'customer_id': row['customer_id'],
                'email': row['email'] if 'email' in row else '',
                'recency_days': float(row['recency_days']),
                'frequency': float(row['frequency']),
                'monetary': float(row['monetary']),
                'r_score': float(row['r_score']),
                'f_score': float(row['f_score']),
                'm_score': float(row['m_score']),
                'rfm_score': float(row['rfm_score']),
                'segment': row['segment'],
                'cluster_id': int(row['cluster_id']) if 'cluster_id' in row else 0,
                'event_time': int(row['event_time'])
            }
            feature_records.append(record)
        
        # Inicializar el cliente de Sagemaker Feature Store Runtime
        featurestore_runtime = boto3.client('sagemaker-featurestore-runtime')
        
        # Actualizar Feature Store en lotes de 100 para evitar throttling
        batch_size = 100
        for i in range(0, len(feature_records), batch_size):
            batch = feature_records[i:i+batch_size]
            
            for record in batch:
                # Ingestar registro individual
                featurestore_runtime.put_record(
                    FeatureGroupName=feature_group_name,
                    Record=[
                        {'FeatureName': key, 'ValueAsString': str(value)}
                        for key, value in record.items()
                    ]
                )
            
            logger.info(f"Actualizados {len(batch)} registros en Feature Store (lote {i//batch_size + 1})")
        
        logger.info(f"Feature Store actualizado con éxito: {len(feature_records)} registros")
        return True
    
    except Exception as e:
        logger.error(f"Error actualizando Feature Store: {str(e)}")
        return False

def calculate_rfm_percentiles(df, k_percentiles):
    """
    Calcula los scores RFM basado en percentiles dinámicos
    
    Args:
        df: DataFrame con métricas RFM (recency_days, frequency, monetary)
        k_percentiles: Lista de percentiles para dividir los scores
    
    Returns:
        DataFrame con scores RFM asignados
    """
    # Crear copia para no modificar el original
    df_rfm = df.copy()
    
    # Convertir percentiles a valores numéricos
    percentiles = [int(p) for p in k_percentiles]
    
    # Calcular puntuaciones R (1 es mejor - menor recencia)
    recency_bins = [0] + [np.percentile(df_rfm['recency_days'], p) for p in percentiles]
    df_rfm['r_score'] = pd.cut(
        df_rfm['recency_days'], 
        bins=recency_bins, 
        labels=range(len(percentiles), 0, -1),
        include_lowest=True
    ).astype(int)
    
    # Calcular puntuaciones F (5 es mejor - mayor frecuencia)
    frequency_bins = [0] + [np.percentile(df_rfm['frequency'], p) for p in percentiles]
    df_rfm['f_score'] = pd.cut(
        df_rfm['frequency'], 
        bins=frequency_bins, 
        labels=range(1, len(percentiles)+1),
        include_lowest=True
    ).astype(int)
    
    # Calcular puntuaciones M (5 es mejor - mayor valor monetario)
    monetary_bins = [0] + [np.percentile(df_rfm['monetary'], p) for p in percentiles]
    df_rfm['m_score'] = pd.cut(
        df_rfm['monetary'], 
        bins=monetary_bins, 
        labels=range(1, len(percentiles)+1),
        include_lowest=True
    ).astype(int)
    
    # Calcular score RFM combinado
    df_rfm['rfm_score'] = df_rfm['r_score'] + df_rfm['f_score'] + df_rfm['m_score']
    
    return df_rfm

def cluster_customers(df, n_clusters):
    """
    Realiza clustering avanzado para identificar segmentos más allá del RFM básico
    
    Args:
        df: DataFrame con métricas RFM
        n_clusters: Número de clusters a identificar
    
    Returns:
        DataFrame con clusters asignados, modelo entrenado
    """
    # Seleccionar características para clustering
    features = ['recency_days', 'frequency', 'monetary', 'r_score', 'f_score', 'm_score']
    X = df[features].copy()
    
    # Normalizar los datos para clustering
    for feature in features:
        X[feature] = (X[feature] - X[feature].mean()) / X[feature].std()
    
    # Entrenar modelo K-means
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df['cluster_id'] = kmeans.fit_predict(X)
    
    return df, kmeans

def assign_segments(df):
    """
    Asigna segmentos de clientes basados en scores RFM y análisis de clusters
    
    Args:
        df: DataFrame con scores RFM y cluster_id
    
    Returns:
        DataFrame con segmentos asignados
    """
    # Definir condiciones para segmentos (basado en puntuación RFM)
    conditions = [
        # Champions: mejores clientes, alta frecuencia y alto valor
        (df['rfm_score'] >= 13),
        # Loyal Customers: compran regularmente con buen valor
        (df['rfm_score'] >= 11) & (df['rfm_score'] < 13),
        # Potential Loyalists: compras recientes con buen potencial
        (df['rfm_score'] >= 9) & (df['rfm_score'] < 11),
        # Promising: compras recientes pero baja frecuencia/valor
        (df['r_score'] >= 4) & (df['rfm_score'] >= 7) & (df['rfm_score'] < 9),
        # Needs Attention: clientes buenos que están en riesgo
        (df['rfm_score'] >= 5) & (df['rfm_score'] < 7),
        # At Risk: clientes regulares que no han comprado recientemente
        (df['rfm_score'] >= 3) & (df['rfm_score'] < 5),
        # Can't Lose: clientes valiosos que no han comprado recientemente
        (df['m_score'] >= 4) & (df['rfm_score'] < 3),
        # Lost: clientes que no han comprado en mucho tiempo
        (df['rfm_score'] < 3)
    ]
    
    # Definir segmentos correspondientes
    segments = [
        'Champions',
        'Loyal Customers',
        'Potential Loyalists',
        'Promising',
        'Needs Attention',
        'At Risk',
        "Can't Lose",
        'Lost'
    ]
    
    # Aplicar segmentación
    df['segment'] = np.select(conditions, segments, default='Undefined')
    
    return df

def main():
    """Función principal para el modelo dinámico de RFM"""
    # Parsear argumentos
    args = parse_args()
    logger.info(f"Iniciando procesamiento RFM dinámico con argumentos: {args}")
    
    # Establecer fecha de análisis
    analysis_date = args.analysis_date
    if not analysis_date:
        analysis_date = datetime.now().strftime("%Y-%m-%d")
    
    # Leer datos de entrada
    logger.info(f"Leyendo datos desde {args.input_data}")
    input_df = read_from_s3(args.input_data)
    logger.info(f"Datos leídos: {len(input_df)} registros")
    
    # Comprobar que tenemos las columnas necesarias
    required_cols = ['customer_id', 'recency_days', 'frequency', 'monetary']
    if not all(col in input_df.columns for col in required_cols):
        logger.error(f"Faltan columnas requeridas. Se necesitan: {required_cols}")
        raise ValueError(f"Los datos de entrada deben contener las columnas: {required_cols}")
    
    # Convertir percentiles de string a lista
    k_percentiles = [int(p) for p in args.k_percentiles.split(',')]
    
    # Calcular scores RFM basados en percentiles dinámicos
    logger.info(f"Calculando scores RFM con percentiles: {k_percentiles}")
    rfm_df = calculate_rfm_percentiles(input_df, k_percentiles)
    
    # Realizar clustering avanzado
    logger.info(f"Realizando clustering con {args.n_clusters} clusters")
    clustered_df, kmeans_model = cluster_customers(rfm_df, args.n_clusters)
    
    # Asignar segmentos basados en RFM y clusters
    logger.info("Asignando segmentos")
    segmented_df = assign_segments(clustered_df)
    
    # Guardar modelo entrenado
    model_path = save_model(
        kmeans_model, 
        args.model_dir, 
        f"rfm_kmeans_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
    )
    
    # Guardar configuración
    config = {
        'model_path': model_path,
        'k_percentiles': k_percentiles,
        'n_clusters': args.n_clusters,
        'analysis_date': analysis_date,
        'processing_timestamp': datetime.now().isoformat()
    }
    
    # Guardar configuración en S3
    s3 = boto3.client('s3')
    bucket, key_prefix = args.model_dir.replace("s3://", "").split("/", 1)
    config_key = f"{key_prefix}/config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    s3.put_object(
        Bucket=bucket,
        Key=config_key,
        Body=json.dumps(config)
    )
    
    # Actualizar Feature Store con las características RFM
    logger.info(f"Actualizando Feature Store: {args.feature_group_name}")
    feature_store_updated = update_feature_store(segmented_df, args.feature_group_name)
    
    # Agregar metadatos y guardar resultados
    segmented_df['processing_date'] = analysis_date
    segmented_df['model_version'] = model_path
    
    # Guardar resultados
    logger.info(f"Guardando resultados en {args.output_data}")
    write_to_s3(segmented_df, args.output_data)
    
    # Generar resumen de segmentos
    segment_summary = segmented_df.groupby('segment').agg(
        customer_count=('customer_id', 'count'),
        avg_recency=('recency_days', 'mean'),
        avg_frequency=('frequency', 'mean'),
        avg_monetary=('monetary', 'mean')
    ).reset_index()
    
    # Agregar porcentaje de clientes por segmento
    total_customers = segmented_df.shape[0]
    segment_summary['percentage'] = segment_summary['customer_count'] / total_customers * 100
    
    # Guardar resumen
    summary_output = args.output_data.replace('.parquet', '_summary.parquet')
    write_to_s3(segment_summary, summary_output)
    
    logger.info("Procesamiento RFM dinámico completado con éxito")

if __name__ == "__main__":
    main()

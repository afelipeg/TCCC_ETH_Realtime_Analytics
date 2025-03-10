import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, datediff, current_date, count, sum, avg, max, row_number, ntile
from pyspark.sql.window import Window
import datetime

# Inicialización de Glue
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'source_path', 
    'destination_path', 
    'database_name',
    'analysis_date'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Definir fecha de análisis (por defecto hoy)
analysis_date = args.get('analysis_date', datetime.datetime.now().strftime("%Y-%m-%d"))
print(f"Fecha de análisis: {analysis_date}")

# 1. EXTRACCIÓN DE DATOS DE MAGENTO
# -------------------------------

# Clientes
customers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args['database_name'],
    table_name="magento_customers"
)

# Pedidos
orders_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args['database_name'],
    table_name="magento_sales_order"
)

# Detalle de pedidos
order_items_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args['database_name'],
    table_name="magento_sales_order_item"
)

# Direcciones de entrega
addresses_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args['database_name'],
    table_name="magento_customer_address"
)

# Convertir DynamicFrames a DataFrames para facilitar el procesamiento
customers_df = customers_dyf.toDF()
orders_df = orders_df.toDF()
order_items_df = order_items_dyf.toDF()
addresses_df = addresses_dyf.toDF()

print(f"Clientes: {customers_df.count()}")
print(f"Pedidos: {orders_df.count()}")
print(f"Items: {order_items_df.count()}")

# 2. CÁLCULO DE MÉTRICAS RFM
# -------------------------

# Filtrar solo pedidos completados
completed_orders_df = orders_df.filter(
    (col("status") == "complete") | 
    (col("status") == "processing")
)

# Análisis RFM a nivel de cliente
rfm_df = completed_orders_df.groupBy("customer_id").agg(
    # Recency: días desde la última compra
    datediff(lit(analysis_date), max("created_at")).alias("recency_days"),
    # Frequency: número total de pedidos
    count("entity_id").alias("frequency"),
    # Monetary: valor promedio de pedido (AOV)
    avg("grand_total").alias("average_order_value"),
    # Valor monetario total
    sum("grand_total").alias("total_spent"),
    # Primera compra
    min("created_at").alias("first_order_date"),
    # Última compra
    max("created_at").alias("last_order_date")
)

# Añadir métricas derivadas
rfm_df = rfm_df.withColumn(
    "customer_lifetime_days", 
    datediff(col("last_order_date"), col("first_order_date"))
)

rfm_df = rfm_df.withColumn(
    "average_order_frequency_days",
    F.when(col("frequency") > 1, 
           col("customer_lifetime_days") / (col("frequency") - 1)
    ).otherwise(None)
)

# 3. SEGMENTACIÓN DE CLIENTES
# -------------------------

# Crear cuartiles para cada dimensión RFM (1 es mejor, 4 es peor)
window_spec = Window.orderBy(col("recency_days"))
rfm_df = rfm_df.withColumn("r_score", ntile(4).over(window_spec.orderBy(col("recency_days").desc())))

window_spec = Window.orderBy(col("frequency"))
rfm_df = rfm_df.withColumn("f_score", ntile(4).over(window_spec.orderBy(col("frequency"))))

window_spec = Window.orderBy(col("average_order_value"))
rfm_df = rfm_df.withColumn("m_score", ntile(4).over(window_spec.orderBy(col("average_order_value"))))

# Calcular puntuación RFM combinada
rfm_df = rfm_df.withColumn("rfm_score", col("r_score") + col("f_score") + col("m_score"))

# Definir segmentos según puntuación RFM
rfm_segments = rfm_df.withColumn(
    "segment",
    F.when(col("rfm_score") >= 9, "Champions")
    .when(col("rfm_score") >= 7, "Loyal Customers")
    .when(col("rfm_score") >= 5, "Potential Loyalists")
    .when(col("rfm_score") >= 4, "At Risk")
    .when(col("rfm_score") >= 3, "Can't Lose Them")
    .otherwise("Lost")
)

# Añadir fecha de procesamiento
rfm_segments = rfm_segments.withColumn("processing_date", lit(analysis_date))

# 4. ENRIQUECER CON DATOS DE CLIENTES
# ---------------------------------

# Unir con datos demográficos de clientes
enriched_segments = rfm_segments.join(
    customers_df.select(
        "entity_id", 
        "email", 
        "firstname", 
        "lastname", 
        "gender", 
        "dob", 
        "created_at"
    ),
    rfm_segments["customer_id"] == customers_df["entity_id"],
    "left"
)

# Añadir datos de ubicación (última dirección de envío)
latest_addresses = addresses_df.withColumn(
    "address_rank", 
    row_number().over(Window.partitionBy("parent_id").orderBy(col("updated_at").desc()))
)
latest_addresses = latest_addresses.filter(col("address_rank") == 1)

# Unir con direcciones
customer_segments = enriched_segments.join(
    latest_addresses.select(
        "parent_id", 
        "city", 
        "region", 
        "postcode", 
        "country_id"
    ),
    enriched_segments["customer_id"] == latest_addresses["parent_id"],
    "left"
)

# 5. ESCRIBIR RESULTADOS PARA ACTIVACIÓN DE CAMPAÑAS
# -----------------------------------------------

# Convertir de vuelta a DynamicFrame
segments_dyf = DynamicFrame.fromDF(customer_segments, glueContext, "segments_dyf")

# Guardar segmentos para activación de campañas
glueContext.write_dynamic_frame.from_options(
    frame=segments_dyf,
    connection_type="s3",
    connection_options={
        "path": f"{args['destination_path']}rfm_segments/",
        "partitionKeys": ["segment", "processing_date"]
    },
    format="parquet"
)

# 6. CREAR TABLA DE ACTIVACIÓN PARA AEM
# -----------------------------------

# Preparar datos para activación de campañas en AEM
aem_activation = customer_segments.select(
    "customer_id",
    "email",
    "firstname",
    "lastname",
    "segment",
    "r_score",
    "f_score",
    "m_score",
    "rfm_score",
    "recency_days",
    "frequency",
    "average_order_value",
    "total_spent",
    "city",
    "region",
    "country_id",
    "processing_date"
)

# Convertir a DynamicFrame
aem_activation_dyf = DynamicFrame.fromDF(aem_activation, glueContext, "aem_activation_dyf")

# Guardar datos para activación de AEM
glueContext.write_dynamic_frame.from_options(
    frame=aem_activation_dyf,
    connection_type="s3",
    connection_options={
        "path": f"{args['destination_path']}aem_activation/",
        "partitionKeys": ["segment", "processing_date"]
    },
    format="parquet"
)

# 7. ANALÍTICA AGREGADA PARA DASHBOARDS
# ----------------------------------

# Calcular distribución de segmentos
segment_distribution = customer_segments.groupBy("segment").agg(
    count("*").alias("customer_count"),
    sum("total_spent").alias("segment_total_revenue"),
    avg("frequency").alias("segment_avg_frequency"),
    avg("average_order_value").alias("segment_avg_order_value"),
    avg("recency_days").alias("segment_avg_recency")
)

# Añadir porcentaje de clientes por segmento
total_customers = customer_segments.count()
segment_distribution = segment_distribution.withColumn(
    "segment_percentage", 
    (col("customer_count") / total_customers) * 100
)

# Convertir a DynamicFrame
segment_analytics_dyf = DynamicFrame.fromDF(segment_distribution, glueContext, "segment_analytics_dyf")

# Guardar datos para dashboards
glueContext.write_dynamic_frame.from_options(
    frame=segment_analytics_dyf,
    connection_type="s3",
    connection_options={
        "path": f"{args['destination_path']}segment_analytics/",
        "partitionKeys": ["processing_date"]
    },
    format="parquet"
)

# Finalizar el job
job.commit()

# TCCC_ETH_Realtime_Analytics
La solución implementa una arquitectura serverless completa para analítica descriptiva y reportes en tiempo real

Componentes de la Arquitectura
1. Ingesta de Datos:
  API Gateway + Lambda para recibir datos a través de solicitudes HTTP
  Kinesis Data Stream para procesamiento en tiempo real
  Cada fuente (Magento, Adobe Experience Manager, Google Analytics, Fanplayr, MySQL) puede enviar datos a través de la API
2. Procesamiento:
  Kinesis Firehose para canalizar datos a S3 con particionamiento dinámico
  Lambda para transformaciones en tiempo real
  AWS Glue para ETL más complejo y catalogado de datos

3. Almacenamiento:
  S3 en dos capas (Raw y Curated) para almacenamiento económico
  DynamoDB para metadatos y búsquedas rápidas
  Glue Data Catalog para organizar el catálogo de datos


4. Consulta:
  Athena para análisis SQL ad hoc sobre los datos en S3
  Lambda para reverse ETL (enviar datos procesados de vuelta a sistemas fuente)

5. Visualización:
  Compatible con Tableau, Power BI y Looker a través de las APIs de Athena
  El modelo de datos está optimizado para consultas analíticas

Instrucciones de Implementación

1. Despliegue el Stack de CloudFormation
   aws cloudformation deploy --template-file template.yaml --stack-name analytics-platform --capabilities CAPABILITY_IAM

2. Configure las fuentes de datos:
  Implemente conectores específicos para cada fuente (Magento, AEM, etc.)
  Utilice la API Gateway para ingerir datos al sistema


3. Desarrollo del Job de AWS Glue:
  Cree el script ETL en Python para AWS Glue que realizará la transformación de datos
  Súbalo a la ubicación especificada en S3

4. Configuración de visualizadores:
  Configure Tableau/Power BI/Looker para conectarse a Athena usando el controlador JDBC/ODBC
  Cree dashboards basados en las tablas definidas en el catálogo de Glue

INGESTA DIARIA PROGRAMADA
1. Ingesta Diaria Programada
He añadido una Lambda ScheduledIngestionLambda que se activa diariamente a la 1 AM UTC para extraer datos de todas las fuentes:
  Conecta con Magento Commerce
  Extrae datos de Adobe Experience Manager
  Obtiene métricas de Google Analytics
  Recopila datos de Fanplayr
  Extrae cambios del sistema MySQL

//Esta Lambda se integra con una regla de EventBridge (DailyDataIngestionRule) para ejecución automática.
2. Procesamiento ETL Mejorado
He implementado un proceso ETL más robusto:
  La Lambda ETLLambda ahora detecta automáticamente la fuente de datos y fecha de los archivos
  El Job de Glue (ETLGlueJob) aplica transformaciones específicas según el tipo de fuente
  Se genera automáticamente el script de ETL y se despliega a S3 durante la creación de la infraestructura

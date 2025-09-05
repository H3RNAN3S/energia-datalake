"""
Parametros del Job
:param destination_db_name: Nombre de la base de datos destino
:param destination_table_name_proveedores_clean: Nombre de la tabla destino proveedores
:param destination_table_name_clientes_clean: Nombre de la tabla destino clientes
:param destination_table_name_transacciones_clean: Nombre de la tabla destino transacciones
:param env: Entorno de ejecucion del job (dev, qa, prod)
:param artifact_bucket: Nombre del bucket donde se encuentran los artefactos
:param folder_target: Carpeta destino dentro del bucket de artefactos
:param bucket_parameter: Nombre del bucket donde se encuentran los archivos fuente
:param path_file_proveedores: Ruta del archivo proveedores dentro del bucket fuente
:param path_file_clientes: Ruta del archivo clientes dentro del bucket fuente
:param path_file_transacciones: Ruta del archivo transacciones dentro del bucket fuente
"""

# Nativas
from abc import ABC, abstractmethod
import datetime
import logging
from pytz import timezone
from sys import argv, stdout, exc_info
import time
from typing import List,Dict

# AWS
from awsglue.utils import getResolvedOptions

# De terceros
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when,current_timestamp, concat_ws, row_number, array,expr, upper, trim, to_date,coalesce
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    IntegerType, BooleanType, DateType, TimestampType
)
from pyspark.sql.window import Window
from pyspark.sql import Row


# Variables de tiempo
start_time = time.time()
date_time = datetime.datetime.now(timezone("America/Bogota"))
timer_date = date_time.strftime("%Y-%m-%dT%H:%M:%S-05:00")
date = date_time - datetime.timedelta(1)
previous_date = datetime.datetime.strftime(date, "%Y%m%d")
year = datetime.datetime.strftime(date, "%Y")
month = datetime.datetime.strftime(date, "%m")
day = datetime.datetime.strftime(date, "%d")
today_date = date_time.strftime("%Y-%m-%d")

ERROR_MSG_LOG_FORMAT = "{} (línea: {}, {}): {}."
LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"


# Configurar el logger globalmente al inicio del script
def setup_logging(log_level: int) -> logging.Logger:
    """Configura el logger principal."""
    logger = logging.getLogger()
    logger.handlers.clear()
    handler = logging.StreamHandler(stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(handler)
    logger.setLevel(log_level)
    return logger

# Inicializar el logger al inicio del script
logger = setup_logging(logging.INFO)

# ---------------------------------------------------------------------------
# SPARK SESSION
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -----------------------------------------------------------------------------
def create_log_msg(log_msg: str) -> str:
    """Crea un mensaje de log con detalles de excepción si aplica"""
    exception_type, exception_value, traceback = exc_info()
    if not exception_type:
        return f"{log_msg}."
    return ERROR_MSG_LOG_FORMAT.format(log_msg, traceback.tb_lineno, exception_type.__name__, exception_value)

# ------------------------------------------------------------------------------
# OBTENER PARAMETROS
def get_params(parameter_list) -> dict:
    """Obtiene los parametros de entrada del glue
    :parameter_list: Lista de parametros

    :returns: dict: Valor de los parametros
    """
    try:
        logger.info("Obteniendo parametros del glue job ...")
        params = getResolvedOptions(argv, parameter_list)
        logger.info("Todos los parametros fueron encontrados")
        return params
    except Exception as e:
        error_msg = (f"No se encontraron todos los parametros solicitados: {parameter_list}: {str(e)}")
        logger.error(create_log_msg(error_msg))
        raise

# --------------------------------------------------------------------------------

params_glue = get_params(["destination_db_name", "destination_table_name_proveedores_clean",
                        "env", "artifact_bucket", "folder_target",
                                "bucket_parameter","destination_table_name_clientes_clean",
                                "destination_table_name_transacciones_clean",
                                "path_file_transacciones","path_file_clientes","path_file_proveedores"])

destination_db_name = params_glue["destination_db_name"]
env = params_glue["env"]
artifact_bucket = params_glue["artifact_bucket"].replace('env', env)
folder_target = params_glue["folder_target"]
bucket_parameter = params_glue["bucket_parameter"].replace('env', env)
destination_table_name_proveedores_clean = params_glue["destination_table_name_proveedores_clean"]
destination_table_name_clientes_clean = params_glue["destination_table_name_clientes_clean"]
destination_table_name_transacciones_clean= params_glue["destination_table_name_transacciones_clean"]
path_file_proveedores= params_glue["path_file_proveedores"]
path_file_clientes= params_glue["path_file_clientes"]
path_file_transacciones= params_glue["path_file_transacciones"]

# ------------------------------------------------------------------------------------

# EXTRACCION DE INFORMACION
class DataReader():

    def define_schema_df(self,df: DataFrame):
        """
            Define el Schema de el daframe leido
            :param daframe: daframe leido
        """
        try:
            # Definir el esquema
            schema = StructType([
                StructField("", StringType(), True),
                StructField("", StringType(), True),
                StructField("", StringType(), True),
                StructField("", StringType(), True),
                StructField("", StringType(), True)
            ])
            # Aplicar casting según el schema definido
            for field in schema.fields:
                df = df.withColumn(field.name, df[field.name].cast(field.dataType))

            logger.info(f"se realizo el casteo correctamente")
            return df
        except Exception as e:
            logger.error(f"No se pudo realizar el casteo de la tabla - error en el proceso: {str(e)}")
            return None
 
    def read_csv(self, bucket, path, separator = ','):
        """
        Lee la informacion fuente de tipo archivo plano (CSV)
        :bucket: Nombre del bucket
        :path: Ruta donde se encuentra el archivo a extraer
        :return: DataFrame con los datos del archivo origen
        """
        try:
            df_file = spark.read.option("header", True) \
                        .option("sep", separator) \
                        .csv(f"s3://{bucket}/{path}")
            return df_file
        except Exception as e:
            error_msg = f"Error al leer archivo CSV:: {str(e)}"
            logger.error(create_log_msg(error_msg))
            raise

    def create_db(self):
        """
        Crea la base de datos si no existe
        """
        logger.info("Creando la base de datos si no existe...")
        try:
            logger.info("Se esta creando la base de datos...")
            logger.info(f"CREATE DATABASE {destination_db_name}")
            spark.sql(
                f"CREATE DATABASE IF NOT EXISTS {destination_db_name} LOCATION 's3://{artifact_bucket}/{folder_target}/'")
            logger.info("La base de datos ha sido creada (si no existia)")
        except Exception as e:
            error_msg = f"No se pudo crear/verificar la tabla {destination_db_name}: {str(e)}"
            logger.error(create_log_msg(error_msg))
            raise

# ------------------------------------------------------------------------------------
# CARGA DE INFORMACION
class DataWriter:
    def __init__(self, df):
        self.df = df

    def write_parquet(self, stg_table):
        self.df = self.df.withColumn("partition_0", lit(year)) \
                        .withColumn("partition_1", lit(month)) \
                        .withColumn("partition_2", lit(day))

        catalog_db = destination_db_name
        table_name = stg_table
        partition_by = ["partition_0", "partition_1", "partition_2"]

        logger.info(f"Escribiendo en Parquet como tabla: {catalog_db}.{table_name}")

        self.df.write.format("parquet") \
            .partitionBy(*partition_by) \
            .mode('overwrite') \
            .saveAsTable(f"{catalog_db}.{table_name}")

class Transformer:
    def __init__(self):
        self.df = None

    def transform_data(self):
        try:
            logger.info("Iniciando la transformación de datos...")
        
            reader = DataReader()
            # Transformación de Proveedores
            df_proveedores = reader.read_csv(bucket_parameter, path_file_proveedores)

            df_proveedores_clean = df_proveedores.select(
                col("nombre_proveedor").alias("proveedor_nombre"),
                col("tipo_energia").alias("energia_tipo"),
                col("capacidad_mw").cast("integer").alias("capacidad_megavatios"),
                to_date(col("fecha_contrato"), "yyyy-MM-dd").alias("fecha_inicio_contrato"),
                current_timestamp().alias("fecha_procesamiento")
            ).filter(col("capacidad_megavatios") > 0)

            # Escribir en formato Parquet
            writer = DataWriter(df_proveedores_clean)
            writer.write_parquet(destination_table_name_proveedores_clean)

            # Transformación de Clientes
            df_clientes = reader.read_csv(bucket_parameter, path_file_clientes)

            df_clientes_clean = df_clientes.select(
                col("identificacion").alias("cliente_id"),
                col("tipo_identificacion").alias("tipo_doc"),
                upper(col("nombre")).alias("cliente_nombre"),
                upper(col("ciudad")).alias("ciudad_residencia"),
                col("tipo_cliente").alias("segmento_cliente"),
                current_timestamp().alias("fecha_procesamiento")
            ).filter(col("identificacion").isNotNull())

            # Escribir en formato Parquet
            writer = DataWriter(df_clientes_clean)
            writer.write_parquet(destination_table_name_clientes_clean)

            # Transformación de Transacciones
            df_transacciones = reader.read_csv(bucket_parameter, path_file_transacciones)

            df_transacciones_clean = df_transacciones.select(
                col("tipo_transaccion"),
                col("nombre_cliente_proveedor").alias("contraparte"),
                col("cantidad_kwh").cast("double").alias("cantidad_energia_kwh"),
                col("precio_kwh").cast("double").alias("precio_unitario"),
                (col("cantidad_kwh").cast("double") * col("precio_kwh").cast("double")).alias("valor_total"),
                col("tipo_energia").alias("fuente_energia"),
                to_date(col("fecha_transaccion"), "yyyy-MM-dd").alias("fecha_transaccion"),
                current_timestamp().alias("fecha_procesamiento")
            ).filter(col("cantidad_kwh").cast("double") > 0)

            # Escribir en formato Parquet
            writer = DataWriter(df_transacciones_clean)
            writer.write_parquet(destination_table_name_transacciones_clean)

            logger.info("Transformación de datos completada exitosamente.")
        except Exception as e:
                logger.error(create_log_msg(f"Error Transformación de datos: {e}"))
                raise

# -------------------------------------------------------------------------------------
# MANEJO DEL PROCESO ETL
class Manager:
    def __init__(self):
        self.reader = DataReader()
        self.transfor = Transformer()

    def run(self):
        try:
            logger.info("Empieza procesamiento de la ETL")
            reader = DataReader()
            transfor = Transformer()
            
            # Crea la base de datos en glue catalog
            reader.create_db()

            # Realiza la transformacion de los datos
            transfor.transform_data()
            logger.info("El proceso ETL finalizo correctamente")
            return None
        except Exception as e:
            logger.error(create_log_msg(f"Error en el flujo de ETL: {e}"))
            raise

def main():
    obj_manager = Manager()
    obj_manager.run()

if __name__ == "__main__":
    # El logger ya está inicializado al principio del script
    main()
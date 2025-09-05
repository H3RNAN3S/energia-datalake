from pyathena import connect
import pandas as pd

# Conexión más simple
conn = connect(
    s3_staging_dir='s3://storage-thr-dev/athena-results/',
    region_name='us-east-1',
    database='energia_stg'
)

# Ejecutar consulta directamente
df = pd.read_sql("""
    SELECT 
        fuente_energia,
        SUM(cantidad_energia_kwh) as total_energia,
        AVG(precio_unitario) as precio_promedio
    FROM energia_stg.transacciones_clean
    GROUP BY fuente_energia
    ORDER BY total_energia DESC
""", conn)

print(df)
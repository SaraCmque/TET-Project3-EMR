import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, corr
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import boto3
import io
from datetime import datetime
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, MapType, BooleanType, IntegerType
from pyspark.sql import Row

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Inicializando sesión de Spark...")
    spark = SparkSession.builder \
        .appName("MedellinIncidentsAnalysis") \
        .getOrCreate()
   
    try:
        logging.info("Leyendo datos desde S3...")
        df_spark = spark.read.parquet("s3a://eafit-project-3-bucket/trusted/joined_data/*.parquet")
       
        logging.info("Realizando análisis descriptivo con SparkSQL...")
        descriptive_analysis(spark, df_spark)
       
        logging.info("Convirtiendo a Pandas DataFrame para visualización...")
        df_pandas = df_spark.toPandas()
       
        logging.info("Generando visualizaciones...")
        generate_visualizations(spark, df_pandas)
       
        logging.info("Proceso completado exitosamente!")
       
    except Exception as e:
        logging.error(f"Error en el proceso: {str(e)}")
    finally:
        spark.stop()

def descriptive_analysis(spark, df):
    df.createOrReplaceTempView("incidents")
   
    print("\n=== Estadísticas Descriptivas ===")
    global_stats = spark.sql("""
    SELECT
        CAST(MIN(reported_incidents) AS INT) as min_incidents,
        CAST(MAX(reported_incidents) AS INT) as max_incidents,
        CAST(AVG(reported_incidents) AS DOUBLE) as avg_incidents,
        CAST(STDDEV(reported_incidents) AS DOUBLE) as std_incidents,
        CAST(MIN(max_temp) AS DOUBLE) as min_temp,
        CAST(MAX(max_temp) AS DOUBLE) as max_temp,
        CAST(AVG(max_temp) AS DOUBLE) as avg_temp,
        CAST(MIN(precipitation) AS DOUBLE) as min_precip,
        CAST(MAX(precipitation) AS DOUBLE) as max_precip,
        CAST(AVG(precipitation) AS DOUBLE) as avg_precip
    FROM incidents
    """)
    global_stats.show()
   
    print("\n=== Incidentes por Nivel de Congestión ===")
    congestion_stats = spark.sql("""
    SELECT
        congestion_level_num,
        COUNT(*) as days_count,
        AVG(reported_incidents) as avg_incidents,
        AVG(max_temp) as avg_temp,
        AVG(precipitation) as avg_precip
    FROM incidents
    GROUP BY congestion_level_num
    ORDER BY congestion_level_num
    """)
    congestion_stats.show()
   
    print("\n=== Correlaciones ===")
    correlations = spark.sql("""
    SELECT
        corr(reported_incidents, max_temp) as temp_corr,
        corr(reported_incidents, precipitation) as precip_corr,
        corr(reported_incidents, congestion_level_num) as congestion_corr
    FROM incidents
    """)
    correlations.show()
   
    logging.info("Guardando resultados analíticos en S3...")
    global_stats.write.parquet("s3a://eafit-project-3-bucket/refined/descriptive_stats/global_stats", mode="overwrite")
    congestion_stats.write.parquet("s3a://eafit-project-3-bucket/refined/descriptive_stats/congestion_stats", mode="overwrite")
    correlations.write.parquet("s3a://eafit-project-3-bucket/refined/descriptive_stats/correlations", mode="overwrite")

def generate_visualizations(spark, df):
    df['reported_incidents'] = df['reported_incidents'].astype(int)
    df['max_temp_interval'] = pd.cut(df['max_temp'], bins=8)
    df['precipitation_interval'] = pd.cut(df['precipitation'], bins=8)

    sns.set_style("whitegrid")
    s3 = boto3.client('s3')
    bucket_name = 'eafit-project-3-bucket'
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    viz_metadata = []

    def register_visualization(title, viz_type, s3_key, buffer, width, height, 
                             temp_rel, precip_rel, cong_rel, corr_rel, params):
        file_size_kb = len(buffer.getvalue()) / 1024
        viz_metadata.append({
            'visualization_id': f'{viz_type}_{timestamp}',
            'title': title,
            'visualization_type': viz_type,
            's3_location': f'https://{bucket_name}.s3.us-east-1.amazonaws.com/{s3_key}',
            'file_size_kb': file_size_kb,
            'dimensions': {'width': width, 'height': height},
            'related_data': {
                'temperature': temp_rel,
                'precipitation': precip_rel,
                'congestion': cong_rel,
                'correlation': corr_rel
            },
            'parameters': params
        })

    plt.figure(figsize=(12, 6))
    sns.barplot(data=df, x='max_temp_interval', y='reported_incidents',
                hue='congestion_level_num', palette='Set2', errorbar=None)
    plt.title('Total de Incidentes por Rango de Temperatura y Nivel de Congestión')
    plt.xlabel('Temperatura Máxima (°C)')
    plt.ylabel('Total de Incidentes Reportados')
    plt.xticks(rotation=45)
    plt.tight_layout()
   
    img_buffer1 = io.BytesIO()
    plt.savefig(img_buffer1, format='png', bbox_inches='tight')
    img_buffer1.seek(0)
    img1_key = f'refined/visualizations/temp_vs_incidents_{timestamp}_1.png'
    s3.put_object(Bucket=bucket_name, Key=img1_key, Body=img_buffer1)
    
    register_visualization(
        title='Incidentes por Temperatura y Congestión',
        viz_type='temp_incidents_bar',
        s3_key=img1_key,
        buffer=img_buffer1,
        width=1200,
        height=600,
        temp_rel=True,
        precip_rel=False,
        cong_rel=True,
        corr_rel=False,
        params={
            'plot_type': 'barplot',
            'x_variable': 'max_temp_interval',
            'y_variable': 'reported_incidents',
            'hue_variable': 'congestion_level_num',
            'bins': '8'
        }
    )
    plt.close()
   
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df, x='precipitation_interval', y='reported_incidents',
                hue='congestion_level_num', palette='Set2', errorbar=None)
    plt.title('Total de Incidentes por Rango de Precipitación y Nivel de Congestión')
    plt.xlabel('Precipitación (mm)')
    plt.ylabel('Total de Incidentes Reportados')
    plt.xticks(rotation=45)
    plt.tight_layout()
   
    img_buffer2 = io.BytesIO()
    plt.savefig(img_buffer2, format='png', bbox_inches='tight')
    img2_key = f'refined/visualizations/precip_vs_incidents_{timestamp}_2.png'
    img_buffer2.seek(0)
    s3.put_object(Bucket=bucket_name, Key=img2_key, Body=img_buffer2)
    
    register_visualization(
        title='Incidentes por Precipitación y Congestión',
        viz_type='precip_incidents_bar',
        s3_key=img2_key,
        buffer=img_buffer2,
        width=1200,
        height=600,
        temp_rel=False,
        precip_rel=True,
        cong_rel=True,
        corr_rel=False,
        params={
            'plot_type': 'barplot',
            'x_variable': 'precipitation_interval',
            'y_variable': 'reported_incidents',
            'hue_variable': 'congestion_level_num',
            'bins': '8'
        }
    )
    plt.close()
   
    numeric_df = df[['reported_incidents', 'max_temp', 'precipitation', 'congestion_level_num']]
    corr_matrix = numeric_df.corr(method='pearson')
   
    plt.figure(figsize=(8, 6))
    sns.heatmap(
        corr_matrix,
        annot=True,
        cmap='coolwarm',
        vmin=-1,
        vmax=1,
        linewidths=0.5,
        fmt=".2f"
    )
    plt.title('Matriz de Correlación: Incidentes, Clima y Congestión', pad=20)
    plt.xticks(rotation=45)
    plt.yticks(rotation=0)
    plt.tight_layout()
   
    img_buffer3 = io.BytesIO()
    plt.savefig(img_buffer3, format='png', bbox_inches='tight')
    img3_key = f'refined/visualizations/cor_matrix_{timestamp}_3.png'
    img_buffer3.seek(0)
    s3.put_object(Bucket=bucket_name, Key=img3_key, Body=img_buffer3)
    
    register_visualization(
        title='Matriz de Correlación',
        viz_type='correlation_heatmap',
        s3_key=img3_key,
        buffer=img_buffer3,
        width=800,
        height=600,
        temp_rel=True,
        precip_rel=True,
        cong_rel=True,
        corr_rel=True,
        params={
            'plot_type': 'heatmap',
            'method': 'pearson',
            'variables': 'reported_incidents,max_temp,precipitation,congestion_level_num'
        }
    )
    plt.close()
 
    df['congestion_level_str'] = df['congestion_level_num'].astype(str)
   
    plt.figure(figsize=(8, 5))
    sns.barplot(
        data=df,
        x='congestion_level_str',
        y='reported_incidents',
        estimator=np.sum,
        palette='Set2',
        errorbar=None
    )
    plt.title('Total de Incidentes por Nivel de Congestión')
    plt.xlabel('Nivel de Congestión')
    plt.ylabel('Total de Incidentes Reportados')
    plt.tight_layout()
   
    img_buffer4 = io.BytesIO()
    plt.savefig(img_buffer4, format='png', bbox_inches='tight')
    img4_key = f'refined/visualizations/con_vs_in_{timestamp}_4.png'
    img_buffer4.seek(0)
    s3.put_object(Bucket=bucket_name, Key=img4_key, Body=img_buffer4)
    
    register_visualization(
        title='Incidentes por Nivel de Congestión',
        viz_type='congestion_incidents_bar',
        s3_key=img4_key,
        buffer=img_buffer4,
        width=800,
        height=500,
        temp_rel=False,
        precip_rel=False,
        cong_rel=True,
        corr_rel=False,
        params={
            'plot_type': 'barplot',
            'x_variable': 'congestion_level_str',
            'y_variable': 'reported_incidents',
            'estimator': 'sum'
        }
    )
    plt.close()
    
    if viz_metadata:
        schema = StructType([
            StructField("visualization_id", StringType()),
            StructField("title", StringType()),
            StructField("visualization_type", StringType()),
            StructField("s3_location", StringType()),
            StructField("file_size_kb", DoubleType()),
            StructField("dimensions", StructType([
                StructField("width", IntegerType()),
                StructField("height", IntegerType())
            ])),
            StructField("related_data", StructType([
                StructField("temperature", BooleanType()),
                StructField("precipitation", BooleanType()),
                StructField("congestion", BooleanType()),
                StructField("correlation", BooleanType())
            ])),
            StructField("parameters", MapType(StringType(), StringType()))
        ])
        
        metadata_df = spark.createDataFrame(viz_metadata, schema=schema)
        metadata_df.write.parquet(
            "s3a://eafit-project-3-bucket/refined/visualizations_metadata/",
            mode="append"
        )
        logging.info("Metadatos de visualizaciones guardados exitosamente en Athena")

if __name__ == "__main__":
    main()
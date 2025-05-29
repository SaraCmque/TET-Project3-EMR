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
        generate_visualizations(df_pandas)
       
        logging.info("Proceso completado exitosamente!")
       
    except Exception as e:
        logging.error(f"Error en el proceso: {str(e)}")
    finally:
        spark.stop()
 
def descriptive_analysis(spark, df):
    df.createOrReplaceTempView("incidents")
   
    print("\n=== Estadísticas Descriptivas ===")
    spark.sql("""
    SELECT
        MIN(reported_incidents) as min_incidents,
        MAX(reported_incidents) as max_incidents,
        AVG(reported_incidents) as avg_incidents,
        STDDEV(reported_incidents) as std_incidents,
        MIN(max_temp) as min_temp,
        MAX(max_temp) as max_temp,
        AVG(max_temp) as avg_temp,
        MIN(precipitation) as min_precip,
        MAX(precipitation) as max_precip,
        AVG(precipitation) as avg_precip
    FROM incidents
    """).show()
   
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
    congestion_stats.write.parquet("s3a://eafit-project-3-bucket/refined/descriptive_stats/congestion_stats.parquet", mode="overwrite")
    correlations.write.parquet("s3a://eafit-project-3-bucket/refined/descriptive_stats/correlations.parquet", mode="overwrite")
 
def generate_visualizations(df):
    df['reported_incidents'] = df['reported_incidents'].astype(int)
    df['max_temp_interval'] = pd.cut(df['max_temp'], bins=8)
    df['precipitation_interval'] = pd.cut(df['precipitation'], bins=8)
   
    sns.set_style("whitegrid")
   
    s3 = boto3.client('s3')
    bucket_name = 'eafit-project-3-bucket'
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
   
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
    s3.put_object(Bucket=bucket_name,
                 Key=f'refined/visualizations/temp_vs_incidents_{timestamp}_1.png',
                 Body=img_buffer1)
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
    print("Saving precipitation vs incidents plot to S3...")
    print(len(img_buffer2.getvalue()))
    img_buffer2.seek(0)
    s3.put_object(Bucket=bucket_name,
                 Key=f'refined/visualizations/precip_vs_incidents_{timestamp}_2.png',
                 Body=img_buffer2)
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
   
    img_buffer4 = io.BytesIO()
    plt.savefig(img_buffer4, format='png', bbox_inches='tight')
    img_buffer4.seek(0)
    s3.put_object(Bucket=bucket_name,
                 Key=f'refined/visualizations/cor_matrix_{timestamp}_3.png',
                 Body=img_buffer4)
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
   
    img_buffer3 = io.BytesIO()
    print("Saving congestion vs incidents plot to S3...")
    plt.savefig(img_buffer3, format='png', bbox_inches='tight')
    print("Plot saved to buffer, now uploading to S3...")
    print(len(img_buffer3.getvalue()))
    img_buffer3.seek(0)
    s3.put_object(Bucket=bucket_name,
                 Key=f'refined/visualizations/con_vs_in_{timestamp}_4.png',
                 Body=img_buffer3)
    plt.close()
 
if __name__ == "__main__":
    main()
 
 
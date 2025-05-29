from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, arrays_zip, col, when
import sys
import traceback
 
def main():
    try:
        spark = SparkSession.builder \
            .appName("ClimateTrafficETL") \
            .getOrCreate()
 
        spark.sparkContext.setLogLevel("INFO")
        logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        logger.info("===> Iniciando proceso ETL")
 
        logger.info("===> Leyendo datos climáticos desde S3")
        weather_df = spark.read.option("multiline", "true").json("s3://eafit-project-3-bucket/raw/api/*.json")
        
        logger.info(f"===> Datos climáticos leídos. Filas: {weather_df.count()}")
 
        logger.info("===> Leyendo datos de tráfico desde S3")
        traffic_df = spark.read.option("header", "true").csv("s3://eafit-project-3-bucket/raw/mysql/*.csv")
        logger.info(f"===> Datos de tráfico leídos. Filas: {traffic_df.count()}")


        logger.info("===> Procesando datos climáticos")
        exploded = weather_df.select(
            explode(arrays_zip(
                col("daily")["time"],
                col("daily")["temperature_2m_max"],
                col("daily")["precipitation_sum"]
            )).alias("col")
        )

        exploded.printSchema()

        weather_processed = exploded.select(
            col("col.0").cast("date").alias("date"),
            col("col.1").alias("max_temp"),
            col("col.2").alias("precipitation")
        )

        logger.info(f"===> Datos climáticos procesados. Filas: {weather_processed.count()}")
 
        logger.info("===> Transformando y uniendo datos de tráfico")
        traffic_df = traffic_df.withColumn("date", col("incident_date").cast("date"))

        final_df = traffic_df.join(weather_processed, "date", "inner") \
            .withColumn("congestion_level_num", 
                        when(col("congestion_level") == "Bajo", 1)
                        .when(col("congestion_level") == "Medio", 2)
                        .otherwise(3))
        logger.info(f"===> Datos finales unidos. Filas: {final_df.count()}")

        final_df = final_df.drop("congestion_level", "incident_date")

        final_df.printSchema()
 
        logger.info("===> Escribiendo datos finales a S3")
        final_df.write \
            .mode("overwrite") \
            .parquet("s3://eafit-project-3-bucket/trusted/joined_data/")
        logger.info("===> Proceso ETL completado exitosamente")
 
        spark.stop()
 
    except Exception as e:
        print("===> ERROR DETECTADO")
        print(str(e))
        traceback.print_exc()
        sys.exit(1)
 
if __name__ == "__main__":
    main()
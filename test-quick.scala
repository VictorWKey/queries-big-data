// Script de prueba rápida para verificar que Spark carga correctamente los datos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

println("\n=== PRUEBA RÁPIDA DE CARGA DE DATOS ===\n")

val spark = SparkSession.builder()
  .appName("IMDB Test")
  .master("local[*]")
  .config("spark.driver.memory", "4g")
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

val moviesPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv"

println("Cargando IMDb movies.csv...")
val moviesDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .option("escape", "\"")
  .csv(moviesPath)

println(s"\n✓ Total de películas: ${moviesDF.count()}")
println("\n✓ Esquema del dataset:")
moviesDF.printSchema()

println("\n✓ Primeras 5 películas:")
moviesDF.select("title", "year", "genre", "avg_vote").show(5, false)

println("\n✓ Prueba de consulta - Top 5 películas mejor calificadas (con más de 100k votos):")
moviesDF
  .filter(col("votes") > 100000)
  .select("title", "year", "avg_vote", "votes")
  .orderBy(col("avg_vote").desc)
  .show(5, false)

println("\n=== PRUEBA COMPLETADA EXITOSAMENTE ===\n")
println("El sistema está listo para ejecutar las consultas completas.")
println("Ejecuta: ./run-predefined.sh o ./run-interactive.sh\n")

spark.stop()
System.exit(0)

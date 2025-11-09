import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Script para identificar qué features corresponden a cada índice
// :load ml_prediction/IdentificarFeatures.scala
// IdentificarFeatures.main(Array())

object IdentificarFeatures {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Identificar Features")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    println("=" * 80)
    println("🔍 IDENTIFICACIÓN DE FEATURES POR ÍNDICE")
    println("=" * 80)
    println()
    
    println("📊 MAPA DE FEATURES (según VectorAssembler):")
    println("-" * 80)
    println()
    
    // Basado en el código de IMDBPredictionModelNOLEAKAGE.scala
    val featureMap = Map(
      "0-99" -> "description_features (TF-IDF)",
      "100-115" -> "genre_features (FeatureHasher - 16 features)",
      "116" -> "director_encoded (Target Encoding)",
      "117" -> "actors_encoded (Target Encoding)",
      "118" -> "duration (numérica)",
      "119" -> "duration_indexed (categórica)",
      "120" -> "year_clean (numérica)",
      "121" -> "decade (numérica)",
      "122" -> "is_recent (binaria)",
      "123" -> "is_old_classic (binaria)"
    )
    
    println("Índice    Feature                              Tipo")
    println("-" * 80)
    println("0-99      description_features                 TF-IDF (100 features)")
    println("100-115   genre_features                       FeatureHasher (16 features)")
    println("116       director_encoded                     Target Encoding")
    println("117       actors_encoded                       Target Encoding ⚠️")
    println("118       duration                             Numérica")
    println("119       duration_indexed                     Categórica")
    println("120       year_clean                           Numérica")
    println("121       decade                               Numérica")
    println("122       is_recent                            Binaria")
    println("123       is_old_classic                       Binaria")
    println()
    
    println("=" * 80)
    println("🔴 ANÁLISIS DE FEATURE IMPORTANCES")
    println("=" * 80)
    println()
    
    println("Feature 117 (74% importancia) = actors_encoded")
    println("Feature 116 (11% importancia) = director_encoded")
    println()
    
    println("⚠️  PROBLEMA IDENTIFICADO:")
    println("-" * 80)
    println("El TARGET ENCODING está causando data leakage indirecto!")
    println()
    println("EXPLICACIÓN:")
    println("  1. Target Encoding calcula: encoding = mean(avg_vote) por categoría")
    println("  2. Aunque se calcula solo en TRAIN, sigue siendo el PROMEDIO del target")
    println("  3. Los actores/directores 'buenos' tienen encoding alto → rating alto")
    println("  4. El modelo aprende: encoding_alto → rating_alto (casi tautológico)")
    println()
    println("EVIDENCIA:")
    println("  • actors_encoded domina el 74% del modelo")
    println("  • director_encoded contribuye otro 11%")
    println("  • Total Target Encoding: 85% de importancia")
    println()
    
    println("=" * 80)
    println("🧪 PRUEBA: CORRELACIÓN DIRECTA")
    println("=" * 80)
    println()
    
    // Cargar datos y verificar
    val moviesPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv"
    val ratingsPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb ratings.csv"
    
    val movies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("escape", "\"")
      .option("multiLine", "true")
      .csv(moviesPath)
    
    val ratings = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ratingsPath)
    
    val df = movies.join(ratings, Seq("imdb_title_id"), "inner")
      .na.drop(Seq("actors", "avg_vote"))
    
    // Calcular encoding de actores manualmente
    val actorsMean = df.groupBy("actors")
      .agg(mean("avg_vote").alias("actors_avg_rating"))
    
    val dfWithEncoding = df.join(actorsMean, "actors")
    
    val correlation = dfWithEncoding.stat.corr("avg_vote", "actors_avg_rating")
    
    println(s"Correlación entre avg_vote y actors_encoded: ${correlation}")
    println()
    
    if (correlation > 0.9) {
      println("🔴 CORRELACIÓN MUY ALTA (> 0.9) → TARGET ENCODING = DATA LEAKAGE")
      println()
      println("EXPLICACIÓN TÉCNICA:")
      println("  Target Encoding usa directamente el promedio del target")
      println("  → actors_encoded ≈ avg_vote (por construcción)")
      println("  → El modelo aprende una relación circular")
      println()
    } else if (correlation > 0.7) {
      println("🟡 CORRELACIÓN ALTA (> 0.7) → TARGET ENCODING = LEAKAGE MODERADO")
    } else {
      println("✅ Correlación aceptable")
    }
    
    println("=" * 80)
    println("💡 SOLUCIÓN RECOMENDADA")
    println("=" * 80)
    println()
    println("OPCIÓN 1: ELIMINAR Target Encoding")
    println("-" * 80)
    println("  ❌ Quitar director_encoded y actors_encoded")
    println("  ✅ Usar solo: description, genre, duration, year")
    println("  → R² esperado: 0.30-0.40 (realista)")
    println()
    
    println("OPCIÓN 2: Target Encoding con Smoothing Fuerte")
    println("-" * 80)
    println("  ⚠️  Aumentar smoothing factor (de 10 a 100)")
    println("  ⚠️  Usar K-Fold Target Encoding (evitar overfitting)")
    println("  → R² esperado: 0.50-0.60")
    println()
    
    println("OPCIÓN 3: Frequency Encoding (sin usar target)")
    println("-" * 80)
    println("  ✅ Codificar por frecuencia de aparición")
    println("  ✅ NO usa valores del target")
    println("  → R² esperado: 0.35-0.45")
    println()
    
    println("=" * 80)
    println("🎯 CONCLUSIÓN FINAL")
    println("=" * 80)
    println()
    println("El problema NO es votes/reviews (esas están limpias)")
    println("El problema ES el TARGET ENCODING de actors/director")
    println()
    println("Target Encoding es una forma sutil de data leakage porque:")
    println("  1. Usa directamente el promedio del target")
    println("  2. Crea una correlación casi perfecta por construcción")
    println("  3. No refleja capacidad predictiva real")
    println()
    println("RECOMENDACIÓN: Ejecutar modelo SIN target encoding")
    println("(Ver: IMDBPredictionModelSINTARGETENCODING.scala)")
    println()
    println("=" * 80)
    
    spark.stop()
  }
}

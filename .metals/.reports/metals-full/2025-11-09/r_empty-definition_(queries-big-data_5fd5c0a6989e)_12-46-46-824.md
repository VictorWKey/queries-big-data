error id: file://<WORKSPACE>/ml_prediction/VerificarDataLeakage.scala:sql.
file://<WORKSPACE>/ml_prediction/VerificarDataLeakage.scala
empty definition using pc, found symbol in pc: sql.
semanticdb not found
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions/org/apache/spark/sql.
	 -org/apache/spark/sql.
	 -scala/Predef.org.apache.spark.sql.
offset: 68
uri: file://<WORKSPACE>/ml_prediction/VerificarDataLeakage.scala
text:
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql@@.functions._

// Script para verificar correlaciones y detectar data leakage
// :load ml_prediction/VerificarDataLeakage.scala
// VerificarDataLeakage.main(Array())

object VerificarDataLeakage {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Verificación Data Leakage")
      .master("local[*]")
      .config("spark.driver.memory", "8g")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    println("=" * 80)
    println("🔍 ANÁLISIS DE CORRELACIONES - DETECCIÓN DE DATA LEAKAGE")
    println("=" * 80)
    println()
    
    val moviesPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv"
    val ratingsPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb ratings.csv"
    
    // Cargar datos
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
    
    val dfJoined = movies.join(ratings, Seq("imdb_title_id"), "inner")
    
    // Limpiar year y convertir a numérico
    val df = dfJoined
      .withColumn("year_clean", regexp_replace(col("year"), "[^0-9]", "").cast("double"))
      .na.drop(Seq("avg_vote", "votes", "reviews_from_users", "reviews_from_critics", "duration", "year_clean"))
    
    println("📊 Dataset cargado:")
    println(s"   Total de filas: ${df.count()}")
    println()
    
    // ============================================================================
    // ANÁLISIS 1: Correlaciones directas con avg_vote
    // ============================================================================
    println("=" * 80)
    println("🔴 ANÁLISIS 1: CORRELACIONES CON avg_vote (TARGET)")
    println("=" * 80)
    println()
    
    val correlations = Map(
      "votes" -> df.stat.corr("avg_vote", "votes"),
      "reviews_from_users" -> df.stat.corr("avg_vote", "reviews_from_users"),
      "reviews_from_critics" -> df.stat.corr("avg_vote", "reviews_from_critics"),
      "duration" -> df.stat.corr("avg_vote", "duration"),
      "year_clean" -> df.stat.corr("avg_vote", "year_clean")
    )
    
    println(f"${"Variable"}%-25s ${"Correlación"}%-15s ${"Interpretación"}%-30s")
    println("-" * 80)
    
    correlations.toSeq.sortBy(-_._2).foreach { case (variable, corr) =>
      val interpretacion = corr match {
        case c if math.abs(c) > 0.7 => "🔴 DATA LEAKAGE FUERTE"
        case c if math.abs(c) > 0.5 => "🟡 DATA LEAKAGE MODERADO"
        case c if math.abs(c) > 0.3 => "🟠 CORRELACIÓN SOSPECHOSA"
        case _ => "✅ CORRELACIÓN ACEPTABLE"
      }
      println(f"$variable%-25s ${corr}%-15.4f $interpretacion%-30s")
    }
    
    println()
    
    // ============================================================================
    // ANÁLISIS 2: Distribución de votes por rango de rating
    // ============================================================================
    println("=" * 80)
    println("🔴 ANÁLISIS 2: DISTRIBUCIÓN DE VOTES POR RATING")
    println("=" * 80)
    println()
    
    val dfWithRatingBin = df.withColumn(
      "rating_bin",
      when(col("avg_vote") < 5.0, "Muy Bajo (<5)")
        .when(col("avg_vote") < 6.0, "Bajo (5-6)")
        .when(col("avg_vote") < 7.0, "Medio (6-7)")
        .when(col("avg_vote") < 8.0, "Bueno (7-8)")
        .otherwise("Excelente (8+)")
    )
    
    val votesByRating = dfWithRatingBin.groupBy("rating_bin")
      .agg(
        avg("votes").alias("avg_votes"),
        count("*").alias("count")
      )
      .orderBy(desc("avg_votes"))
    
    println(f"${"Rango de Rating"}%-20s ${"Promedio Votos"}%-20s ${"# Películas"}%-15s")
    println("-" * 80)
    
    votesByRating.collect().foreach { row =>
      val bin = row.getString(0)
      val avgVotes = row.getDouble(1)
      val count = row.getLong(2)
      println(f"$bin%-20s ${avgVotes}%-20.0f ${count}%-15d")
    }
    
    println()
    println("⚠️  OBSERVACIÓN:")
    println("   Si películas con mayor rating tienen MÁS votos → DATA LEAKAGE")
    println("   (El modelo aprende: muchos votos = buen rating)")
    println()
    
    // ============================================================================
    // ANÁLISIS 3: Distribución de reviews por rango de rating
    // ============================================================================
    println("=" * 80)
    println("🔴 ANÁLISIS 3: DISTRIBUCIÓN DE REVIEWS POR RATING")
    println("=" * 80)
    println()
    
    val reviewsByRating = dfWithRatingBin.groupBy("rating_bin")
      .agg(
        avg("reviews_from_users").alias("avg_user_reviews"),
        avg("reviews_from_critics").alias("avg_critic_reviews"),
        count("*").alias("count")
      )
      .orderBy(desc("avg_user_reviews"))
    
    println(f"${"Rango de Rating"}%-20s ${"Avg User Reviews"}%-20s ${"Avg Critic Reviews"}%-20s")
    println("-" * 80)
    
    reviewsByRating.collect().foreach { row =>
      val bin = row.getString(0)
      val avgUser = row.getDouble(1)
      val avgCritic = row.getDouble(2)
      println(f"$bin%-20s ${avgUser}%-20.1f ${avgCritic}%-20.1f")
    }
    
    println()
    println("⚠️  OBSERVACIÓN:")
    println("   Si películas con mayor rating tienen MÁS reviews → DATA LEAKAGE")
    println("   (Las reviews se escriben DESPUÉS del rating)")
    println()
    
    // ============================================================================
    // ANÁLISIS 4: Top features por importancia (simulado)
    // ============================================================================
    println("=" * 80)
    println("🔴 ANÁLISIS 4: IMPORTANCIA RELATIVA DE FEATURES")
    println("=" * 80)
    println()
    
    println("Basado en correlaciones absolutas:")
    println("-" * 80)
    
    val featureImportance = correlations.toSeq
      .map { case (name, corr) => (name, math.abs(corr)) }
      .sortBy(-_._2)
    
    featureImportance.foreach { case (feature, importance) =>
      val bar = "█" * (importance * 50).toInt
      val status = if (importance > 0.5 && (feature.contains("vote") || feature.contains("review"))) {
        "🔴 LEAKAGE"
      } else {
        "✅ OK"
      }
      println(f"$feature%-25s ${importance}%.4f $bar $status")
    }
    
    println()
    
    // ============================================================================
    // CONCLUSIONES
    // ============================================================================
    println("=" * 80)
    println("📊 CONCLUSIONES")
    println("=" * 80)
    println()
    
    val maxCorr = correlations.values.max
    val leakageVars = correlations.filter { case (name, corr) => 
      math.abs(corr) > 0.5 && (name.contains("vote") || name.contains("review"))
    }
    
    if (leakageVars.nonEmpty) {
      println("❌ DATA LEAKAGE DETECTADO:")
      println("-" * 80)
      leakageVars.foreach { case (variable, corr) =>
        println(f"   • $variable: correlación = ${corr}%.4f")
      }
      println()
      println("⚠️  RECOMENDACIÓN:")
      println("   1. ELIMINAR estas variables del modelo")
      println("   2. Usar SOLO características intrínsecas (genre, director, duration, etc.)")
      println("   3. Ejecutar IMDBPredictionModelNOLEAKAGE.scala")
      println()
    } else {
      println("✅ NO SE DETECTÓ DATA LEAKAGE SIGNIFICATIVO")
      println("-" * 80)
      println("   Las correlaciones están en rangos aceptables.")
      println()
    }
    
    println("=" * 80)
    println("✅ ANÁLISIS COMPLETADO")
    println("=" * 80)
    
    spark.stop()
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: sql.
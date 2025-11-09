import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor, GBTRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter

// :load ml_prediction/IMDBPredictionModelOptimized.scala
// IMDBPredictionModelOptimized.main(Array())

object IMDBPredictionModelOptimized {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IMDB Rating Prediction - Optimized")
      .master("local[*]")
      .config("spark.driver.memory", "6g")
      .config("spark.executor.memory", "4g")
      .config("spark.memory.fraction", "0.8")
      .config("spark.memory.storageFraction", "0.3")
      .config("spark.sql.shuffle.partitions", "50")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    println("=" * 80)
    println("MODELO DE PREDICCIÓN OPTIMIZADO - CALIFICACIÓN IMDB")
    println("=" * 80)
    println()
    
    val moviesPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv"
    val ratingsPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb ratings.csv"
    
    println("📊 PASO 1: Cargando y preparando datos...")
    val fullDF = cargarYJoinearDatos(spark, moviesPath, ratingsPath)
    
    println("\n🧹 PASO 2: Limpiando datos...")
    val cleanDF = limpiarDatos(fullDF)
    
    println("\n🔧 PASO 3: Dividiendo train/test...")
    val Array(trainData, testData) = cleanDF.randomSplit(Array(0.8, 0.2), seed = 42)
    
    // IMPORTANTE: Cachear y materializar los datos
    trainData.persist(StorageLevel.MEMORY_AND_DISK)
    testData.persist(StorageLevel.MEMORY_AND_DISK)
    
    val trainCount = trainData.count()
    val testCount = testData.count()
    
    println(s"   ✓ Datos de entrenamiento: $trainCount filas")
    println(s"   ✓ Datos de prueba: $testCount filas")
    
    // Paso 4: Modelo Baseline (Linear Regression)
    println("\n📈 PASO 4: Entrenando modelo BASELINE (Linear Regression)...")
    val baselineMetrics = entrenarModeloBaseline(trainData, testData, spark)
    
    // Paso 5: Modelo Principal (Random Forest) - con parámetros optimizados
    println("\n🌲 PASO 5: Entrenando modelo PRINCIPAL (Random Forest)...")
    val rfMetrics = entrenarRandomForestOptimizado(trainData, testData, spark)
    
    // Paso 6: Modelo Adicional (GBT) - simplificado
    println("\n🚀 PASO 6: Entrenando modelo ADICIONAL (Gradient Boosted Trees)...")
    val gbtMetrics = entrenarGBTOptimizado(trainData, testData, spark)
    
    // Paso 7: Reporte
    println("\n📊 PASO 7: Generando reporte comparativo...")
    generarReporteComparativo(baselineMetrics, rfMetrics, gbtMetrics, spark)
    
    // Limpiar cache
    trainData.unpersist()
    testData.unpersist()
    
    println("\n" + "=" * 80)
    println("✅ PROCESO COMPLETADO - Resultados en ml_prediction/resultados/")
    println("=" * 80)
    
    spark.stop()
  }
  
  def cargarYJoinearDatos(spark: SparkSession, moviesPath: String, ratingsPath: String): DataFrame = {
    val moviesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("escape", "\"")
      .csv(moviesPath)
    
    val ratingsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ratingsPath)
    
    val fullDF = moviesDF.join(ratingsDF, Seq("imdb_title_id"), "left")
    
    println(s"   ✓ Dataset completo: ${fullDF.count()} filas")
    
    fullDF
  }
  
  def limpiarDatos(df: DataFrame): DataFrame = {
    val originalCount = df.count()
    
    val selectedDF = df.select(
      col("imdb_title_id"),
      col("title"),
      col("year"),
      col("genre"),
      col("duration"),
      col("director"),
      col("actors"),
      col("description"),
      col("avg_vote").as("label"),
      col("votes"),
      col("reviews_from_users"),
      col("reviews_from_critics")
    )
    
    val withCleanYear = selectedDF.withColumn(
      "year_clean",
      when(col("year").cast("int").isNotNull, col("year").cast("int"))
        .otherwise(lit(null))
    ).drop("year")
    
    val cleanDF = withCleanYear
      .na.drop(Seq(
        "label",
        "genre",
        "director",
        "actors",
        "description",
        "duration",
        "votes",
        "reviews_from_users",
        "reviews_from_critics",
        "year_clean"
      ))
      .filter(
        col("genre") =!= "" &&
        col("director") =!= "" &&
        col("actors") =!= "" &&
        col("description") =!= ""
      )
    
    val finalCount = cleanDF.count()
    println(s"   ✓ Filas originales: $originalCount")
    println(s"   ✓ Filas limpias: $finalCount")
    println(s"   ✓ Pérdida: ${((originalCount - finalCount).toDouble / originalCount * 100).formatted("%.2f")}%%")
    
    cleanDF
  }
  
  def crearPipelineOptimizado(): Pipeline = {
    // 1. TF-IDF para description (REDUCIDO a 50 features)
    val descTokenizer = new Tokenizer()
      .setInputCol("description")
      .setOutputCol("description_words")
    
    val descRemover = new StopWordsRemover()
      .setInputCol("description_words")
      .setOutputCol("description_filtered")
    
    val descHashingTF = new HashingTF()
      .setInputCol("description_filtered")
      .setOutputCol("description_tf")
      .setNumFeatures(50)  // REDUCIDO de 100 a 50
    
    val descIDF = new IDF()
      .setInputCol("description_tf")
      .setOutputCol("description_features")
    
    // NOTA: Removemos TODAS las categóricas (genre, director, actors)
    // porque tienen demasiadas categorías únicas que causan problemas con DecisionTree
    // Usamos SOLO: description (TF-IDF) + features numéricas
    // Esto es válido ya que description contiene información rica sobre la película
    
    // VectorAssembler - Solo texto + numéricas
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "description_features",
        "duration",
        "votes",
        "reviews_from_users",
        "reviews_from_critics",
        "year_clean"
      ))
      .setOutputCol("features")
      .setHandleInvalid("skip")
    
    new Pipeline().setStages(Array(
      descTokenizer,
      descRemover,
      descHashingTF,
      descIDF,
      assembler
    ))
  }
  
  def entrenarModeloBaseline(trainData: DataFrame, testData: DataFrame, spark: SparkSession): Map[String, Double] = {
    val featurePipeline = crearPipelineOptimizado()
    val lr = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.1)  // Regularización
      .setElasticNetParam(0.0)
    
    val pipeline = new Pipeline().setStages(featurePipeline.getStages ++ Array(lr))
    
    println("   ⏳ Entrenando Linear Regression...")
    val startTime = System.currentTimeMillis()
    val model = pipeline.fit(trainData)
    val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    val predictions = model.transform(testData)
    
    val metrics = evaluarModelo(predictions, "Linear Regression (Baseline)", trainingTime)
    
    guardarPrediccionesMuestra(predictions, "ml_prediction/resultados/baseline_predictions.txt")
    
    metrics
  }
  
  def entrenarRandomForestOptimizado(trainData: DataFrame, testData: DataFrame, spark: SparkSession): Map[String, Double] = {
    val featurePipeline = crearPipelineOptimizado()
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(20)       // REDUCIDO de 50 a 20
      .setMaxDepth(8)        // REDUCIDO de 10 a 8
      .setMaxBins(100)       // AUMENTADO para soportar más categorías
      .setMinInstancesPerNode(5)
      .setSubsamplingRate(0.8)
      .setSeed(42)
    
    val pipeline = new Pipeline().setStages(featurePipeline.getStages ++ Array(rf))
    
    println("   ⏳ Entrenando Random Forest (20 árboles, profundidad 8)...")
    val startTime = System.currentTimeMillis()
    val model = pipeline.fit(trainData)
    val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    val predictions = model.transform(testData)
    
    val metrics = evaluarModelo(predictions, "Random Forest", trainingTime)
    
    // Feature Importance
    val rfModel = model.stages.last.asInstanceOf[org.apache.spark.ml.regression.RandomForestRegressionModel]
    println("\n   📊 Feature Importances (Top 10):")
    val importances = rfModel.featureImportances.toArray.zipWithIndex
      .sortBy(-_._1)
      .take(10)
    importances.foreach { case (importance, idx) =>
      println(f"      Feature $idx%3d: ${importance * 100}%.2f%%")
    }
    
    guardarPrediccionesMuestra(predictions, "ml_prediction/resultados/random_forest_predictions.txt")
    
    metrics
  }
  
  def entrenarGBTOptimizado(trainData: DataFrame, testData: DataFrame, spark: SparkSession): Map[String, Double] = {
    val featurePipeline = crearPipelineOptimizado()
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(20)        // REDUCIDO de 30 a 20
      .setMaxDepth(4)        // REDUCIDO de 5 a 4
      .setMaxBins(100)       // AUMENTADO para soportar más categorías
      .setStepSize(0.1)
      .setSeed(42)
    
    val pipeline = new Pipeline().setStages(featurePipeline.getStages ++ Array(gbt))
    
    println("   ⏳ Entrenando Gradient Boosted Trees (20 iteraciones, profundidad 4)...")
    val startTime = System.currentTimeMillis()
    val model = pipeline.fit(trainData)
    val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    val predictions = model.transform(testData)
    
    val metrics = evaluarModelo(predictions, "Gradient Boosted Trees", trainingTime)
    
    // Feature Importance
    val gbtModel = model.stages.last.asInstanceOf[org.apache.spark.ml.regression.GBTRegressionModel]
    println("\n   📊 Feature Importances (Top 10):")
    val importances = gbtModel.featureImportances.toArray.zipWithIndex
      .sortBy(-_._1)
      .take(10)
    importances.foreach { case (importance, idx) =>
      println(f"      Feature $idx%3d: ${importance * 100}%.2f%%")
    }
    
    guardarPrediccionesMuestra(predictions, "ml_prediction/resultados/gbt_predictions.txt")
    
    metrics
  }
  
  def evaluarModelo(predictions: DataFrame, modelName: String, trainingTime: Double): Map[String, Double] = {
    val evaluatorRMSE = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    
    val evaluatorMAE = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("mae")
    
    val evaluatorR2 = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("r2")
    
    val evaluatorMSE = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("mse")
    
    val rmse = evaluatorRMSE.evaluate(predictions)
    val mae = evaluatorMAE.evaluate(predictions)
    val r2 = evaluatorR2.evaluate(predictions)
    val mse = evaluatorMSE.evaluate(predictions)
    
    println(s"\n   ✅ Métricas de $modelName:")
    println(f"      RMSE: $rmse%.4f")
    println(f"      MAE:  $mae%.4f")
    println(f"      R²:   $r2%.4f")
    println(f"      MSE:  $mse%.4f")
    println(f"      Tiempo: $trainingTime%.2f segundos")
    
    Map(
      "rmse" -> rmse,
      "mae" -> mae,
      "r2" -> r2,
      "mse" -> mse,
      "training_time" -> trainingTime
    )
  }
  
  def guardarPrediccionesMuestra(predictions: DataFrame, outputPath: String): Unit = {
    val muestra = predictions
      .select("title", "label", "prediction")
      .withColumn("error", abs(col("label") - col("prediction")))
      .orderBy(col("error").desc)
      .limit(20)
      .collect()
    
    val writer = new PrintWriter(outputPath)
    writer.println("=" * 80)
    writer.println("MUESTRA DE PREDICCIONES (Top 20 mayores errores)")
    writer.println("=" * 80)
    writer.println()
    writer.println(f"${"Título"}%-50s ${"Real"}%6s ${"Pred"}%6s ${"Error"}%6s")
    writer.println("-" * 80)
    
    muestra.foreach { row =>
      val title = row.getString(0).take(47)
      val real = row.getDouble(1)
      val pred = row.getDouble(2)
      val error = row.getDouble(3)
      writer.println(f"$title%-50s $real%6.2f $pred%6.2f $error%6.2f")
    }
    
    writer.close()
    println(s"   ✓ Predicciones guardadas en: $outputPath")
  }
  
  def generarReporteComparativo(
    baselineMetrics: Map[String, Double],
    rfMetrics: Map[String, Double],
    gbtMetrics: Map[String, Double],
    spark: SparkSession
  ): Unit = {
    
    val outputPath = "ml_prediction/resultados/reporte_comparativo.txt"
    val writer = new PrintWriter(outputPath)
    
    writer.println("=" * 80)
    writer.println("REPORTE COMPARATIVO - PREDICCIÓN DE RATING IMDB")
    writer.println("=" * 80)
    writer.println()
    
    writer.println("RESUMEN DE MÉTRICAS")
    writer.println("-" * 80)
    writer.println(f"${"Modelo"}%-30s ${"RMSE"}%10s ${"MAE"}%10s ${"R²"}%10s ${"Tiempo(s)"}%12s")
    writer.println("-" * 80)
    
    writer.println(f"${"Linear Regression (Baseline)"}%-30s ${baselineMetrics("rmse")}%10.4f ${baselineMetrics("mae")}%10.4f ${baselineMetrics("r2")}%10.4f ${baselineMetrics("training_time")}%12.2f")
    writer.println(f"${"Random Forest"}%-30s ${rfMetrics("rmse")}%10.4f ${rfMetrics("mae")}%10.4f ${rfMetrics("r2")}%10.4f ${rfMetrics("training_time")}%12.2f")
    writer.println(f"${"Gradient Boosted Trees"}%-30s ${gbtMetrics("rmse")}%10.4f ${gbtMetrics("mae")}%10.4f ${gbtMetrics("r2")}%10.4f ${gbtMetrics("training_time")}%12.2f")
    
    writer.println()
    writer.println("COMPARACIÓN CON BASELINE")
    writer.println("-" * 80)
    
    val rfImprovement = (baselineMetrics("rmse") - rfMetrics("rmse")) / baselineMetrics("rmse") * 100
    val gbtImprovement = (baselineMetrics("rmse") - gbtMetrics("rmse")) / baselineMetrics("rmse") * 100
    
    writer.println(f"Random Forest vs Baseline:")
    writer.println(f"  - Reducción RMSE: $rfImprovement%.2f%%")
    writer.println(f"  - Mejora R²: ${(rfMetrics("r2") - baselineMetrics("r2")) * 100}%.2f puntos")
    
    writer.println()
    writer.println(f"Gradient Boosted Trees vs Baseline:")
    writer.println(f"  - Reducción RMSE: $gbtImprovement%.2f%%")
    writer.println(f"  - Mejora R²: ${(gbtMetrics("r2") - baselineMetrics("r2")) * 100}%.2f puntos")
    
    writer.println()
    writer.println("=" * 80)
    writer.println("MEJOR MODELO")
    writer.println("=" * 80)
    
    val bestModel = if (rfMetrics("rmse") < gbtMetrics("rmse")) "Random Forest" else "Gradient Boosted Trees"
    val bestRMSE = math.min(rfMetrics("rmse"), gbtMetrics("rmse"))
    val bestR2 = math.max(rfMetrics("r2"), gbtMetrics("r2"))
    
    writer.println(s"🏆 Modelo ganador: $bestModel")
    writer.println(f"   RMSE: $bestRMSE%.4f")
    writer.println(f"   R²: $bestR2%.4f")
    
    writer.close()
    
    println(s"\n   ✓ Reporte guardado en: $outputPath")
    
    println("\n" + "=" * 80)
    println("📊 RESUMEN FINAL")
    println("=" * 80)
    println(f"${"Modelo"}%-30s ${"RMSE"}%10s ${"R²"}%10s")
    println("-" * 80)
    println(f"${"Linear Regression (Baseline)"}%-30s ${baselineMetrics("rmse")}%10.4f ${baselineMetrics("r2")}%10.4f")
    println(f"${"Random Forest"}%-30s ${rfMetrics("rmse")}%10.4f ${rfMetrics("r2")}%10.4f")
    println(f"${"Gradient Boosted Trees"}%-30s ${gbtMetrics("rmse")}%10.4f ${gbtMetrics("r2")}%10.4f")
    println("=" * 80)
    println(s"🏆 Mejor modelo: $bestModel")
    println("=" * 80)
  }
}

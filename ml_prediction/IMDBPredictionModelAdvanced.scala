import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor, GBTRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter

// :load ml_prediction/IMDBPredictionModelAdvanced.scala
// IMDBPredictionModelAdvanced.main(Array())

object IMDBPredictionModelAdvanced {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IMDB Rating Prediction - Advanced")
      .master("local[*]")
      .config("spark.driver.memory", "12g")
      .config("spark.executor.memory", "12g")
      .config("spark.memory.fraction", "0.8")
      .config("spark.memory.storageFraction", "0.2")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.default.parallelism", "200")
      .config("spark.driver.maxResultSize", "4g")
      .config("spark.kryoserializer.buffer.max", "512m")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    println("=" * 80)
    println("MODELO DE PREDICCIÓN AVANZADO - CALIFICACIÓN IMDB")
    println("=" * 80)
    println()
    
    val moviesPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv"
    val ratingsPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb ratings.csv"
    
    println("📊 PASO 1: Cargando y preparando datos...")
    val fullDF = cargarYJoinearDatos(spark, moviesPath, ratingsPath)
    
    println("\n🧹 PASO 2: Limpiando y enriqueciendo datos...")
    val cleanDF = limpiarYEnriquecerDatos(fullDF)
    
    println("\n🎯 PASO 3: Aplicando Feature Engineering Avanzado...")
    val enrichedDF = aplicarFeatureEngineering(cleanDF)
    
    println("\n🔧 PASO 4: Dividiendo train/test (80/20)...")
    val Array(trainData, testData) = enrichedDF.randomSplit(Array(0.8, 0.2), seed = 42)
    
    // Repartir para mejor paralelismo
    val trainOptimized = trainData.repartition(200).persist(StorageLevel.MEMORY_AND_DISK)
    val testOptimized = testData.repartition(200).persist(StorageLevel.MEMORY_AND_DISK)
    
    val trainCount = trainOptimized.count()
    val testCount = testOptimized.count()
    
    println(s"   ✓ Datos de entrenamiento: $trainCount filas")
    println(s"   ✓ Datos de prueba: $testCount filas")
    
    // ============================================================================
    // MODELO 1: Linear Regression (Baseline con L2 regularization)
    // ============================================================================
    println("\n📈 PASO 5: Entrenando modelo BASELINE (Ridge Regression)...")
    val (baselineModel, baselineMetrics, baselineTime) = entrenarModeloBaseline(
      trainOptimized, testOptimized
    )
    imprimirMetricas("Ridge Regression (Baseline)", baselineMetrics, baselineTime)
    guardarPredicciones(
      baselineModel, 
      testOptimized, 
      "ml_prediction/resultados/advanced_baseline_predictions.txt"
    )
    
    // ============================================================================
    // MODELO 2: Random Forest con Grid Search
    // ============================================================================
    println("\n🌲 PASO 6: Entrenando Random Forest con GRID SEARCH...")
    println("   ⚠️  Este paso puede tomar 15-30 minutos...")
    val (rfModel, rfMetrics, rfTime) = entrenarRandomForestConCV(
      trainOptimized, testOptimized
    )
    imprimirMetricas("Random Forest (Grid Search)", rfMetrics, rfTime)
    imprimirFeatureImportances(rfModel, "Random Forest")
    guardarPredicciones(
      rfModel, 
      testOptimized, 
      "ml_prediction/resultados/advanced_rf_predictions.txt"
    )
    
    // ============================================================================
    // MODELO 3: Gradient Boosted Trees con Grid Search
    // ============================================================================
    println("\n🚀 PASO 7: Entrenando GBT con GRID SEARCH...")
    println("   ⚠️  Este paso puede tomar 20-40 minutos...")
    val (gbtModel, gbtMetrics, gbtTime) = entrenarGBTConCV(
      trainOptimized, testOptimized
    )
    imprimirMetricas("Gradient Boosted Trees (Grid Search)", gbtMetrics, gbtTime)
    imprimirFeatureImportances(gbtModel, "GBT")
    guardarPredicciones(
      gbtModel, 
      testOptimized, 
      "ml_prediction/resultados/advanced_gbt_predictions.txt"
    )
    
    // ============================================================================
    // MODELO 4: Ensemble (Promedio Ponderado)
    // ============================================================================
    println("\n🎭 PASO 8: Creando modelo ENSEMBLE...")
    val ensembleMetrics = crearEnsemble(
      baselineModel, rfModel, gbtModel, testOptimized
    )
    imprimirMetricas("Ensemble (0.2*Ridge + 0.3*RF + 0.5*GBT)", ensembleMetrics, 0.0)
    
    // ============================================================================
    // REPORTE FINAL
    // ============================================================================
    println("\n📊 PASO 9: Generando reporte comparativo detallado...")
    generarReporteComparativoAvanzado(
      Map(
        "Ridge Regression" -> (baselineMetrics, baselineTime),
        "Random Forest" -> (rfMetrics, rfTime),
        "Gradient Boosted Trees" -> (gbtMetrics, gbtTime),
        "Ensemble" -> (ensembleMetrics, 0.0)
      )
    )
    
    // Liberar memoria
    trainOptimized.unpersist()
    testOptimized.unpersist()
    
    println("\n" + "=" * 80)
    println("✅ PROCESO COMPLETADO - Resultados en ml_prediction/resultados/")
    println("=" * 80)
    
    spark.stop()
  }
  
  // ==========================================================================
  // FUNCIONES DE CARGA Y LIMPIEZA
  // ==========================================================================
  
  def cargarYJoinearDatos(spark: SparkSession, moviesPath: String, ratingsPath: String): DataFrame = {
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
    
    val joinedDF = movies.join(ratings, Seq("imdb_title_id"), "inner")
    println(s"   ✓ Dataset completo: ${joinedDF.count()} filas")
    
    joinedDF
  }
  
  def limpiarYEnriquecerDatos(df: DataFrame): DataFrame = {
    val originalCount = df.count()
    
    // Limpiar columna year
    val dfYearCleaned = df.withColumn(
      "year_clean",
      regexp_replace(col("year"), "[^0-9]", "").cast(IntegerType)
    )
    
    // Remover nulls en columnas críticas
    val cleanDF = dfYearCleaned.na.drop(Seq(
      "description", "genre", "director", "actors",
      "duration", "avg_vote", "votes", 
      "reviews_from_users", "reviews_from_critics", "year_clean"
    ))
    
    val cleanCount = cleanDF.count()
    val lossPercent = ((originalCount - cleanCount).toDouble / originalCount * 100)
    
    println(s"   ✓ Filas originales: $originalCount")
    println(s"   ✓ Filas limpias: $cleanCount")
    println(f"   ✓ Pérdida: $lossPercent%.2f%%")
    
    cleanDF
  }
  
  def aplicarFeatureEngineering(df: DataFrame): DataFrame = {
    println("   🔧 Aplicando Target Encoding para director...")
    val dfWithDirectorEncoded = targetEncode(df, "director", "avg_vote")
    
    println("   🔧 Aplicando Target Encoding para actors...")
    val dfWithActorsEncoded = targetEncode(dfWithDirectorEncoded, "actors", "avg_vote")
    
    println("   🔧 Reduciendo cardinalidad de genre...")
    val dfWithGenreReduced = reduceCardinality(dfWithActorsEncoded, "genre", topN = 50)
    
    println("   🔧 Creando features derivadas...")
    val enrichedDF = dfWithGenreReduced
      .withColumn("votes_per_review", 
        col("votes") / (col("reviews_from_users") + col("reviews_from_critics") + 1))
      .withColumn("review_ratio", 
        col("reviews_from_users") / (col("reviews_from_critics") + 1))
      .withColumn("decade", 
        (col("year_clean") / 10).cast(IntegerType) * 10)
      .withColumn("is_recent", 
        when(col("year_clean") >= 2015, 1.0).otherwise(0.0))
      .withColumn("is_old_classic", 
        when(col("year_clean") <= 1980, 1.0).otherwise(0.0))
      .withColumn("log_votes", 
        log1p(col("votes")))
      .withColumn("duration_category",
        when(col("duration") <= 90, "short")
        .when(col("duration") <= 120, "medium")
        .otherwise("long"))
    
    println("   ✓ Features enriquecidas creadas")
    enrichedDF
  }
  
  // Target Encoding: Codifica categorías por su promedio de target
  def targetEncode(df: DataFrame, column: String, targetCol: String): DataFrame = {
    val globalMean = df.select(mean(targetCol)).first().getDouble(0)
    
    val avgByCategory = df.groupBy(column)
      .agg(
        mean(targetCol).alias(s"${column}_encoded"),
        count("*").alias(s"${column}_count")
      )
    
    // Smoothing: combinar promedio de categoría con promedio global
    val smoothingFactor = 10.0
    val avgSmoothed = avgByCategory.withColumn(
      s"${column}_encoded",
      (col(s"${column}_encoded") * col(s"${column}_count") + globalMean * smoothingFactor) /
      (col(s"${column}_count") + smoothingFactor)
    ).drop(s"${column}_count")
    
    df.join(avgSmoothed, Seq(column), "left")
      .na.fill(globalMean, Seq(s"${column}_encoded"))
      .drop(column)
  }
  
  // Reducir cardinalidad agrupando categorías raras en "Other"
  def reduceCardinality(df: DataFrame, column: String, topN: Int): DataFrame = {
    val topCategories = df.groupBy(column)
      .count()
      .orderBy(desc("count"))
      .limit(topN)
      .select(column)
      .collect()
      .map(_.getString(0))
      .toSet
    
    val categorizeUDF = udf((value: String) => 
      if (topCategories.contains(value)) value else "Other"
    )
    
    df.withColumn(column, categorizeUDF(col(column)))
  }
  
  // ==========================================================================
  // CREACIÓN DE PIPELINES
  // ==========================================================================
  
  def crearPipelineAvanzado(regressor: org.apache.spark.ml.Estimator[_]): Pipeline = {
    // 1. TF-IDF para description (con bi-gramas)
    val descTokenizer = new RegexTokenizer()
      .setInputCol("description")
      .setOutputCol("desc_words")
      .setPattern("\\W+")
      .setMinTokenLength(3)
    
    val descRemover = new StopWordsRemover()
      .setInputCol("desc_words")
      .setOutputCol("desc_filtered")
    
    val descNGram = new NGram()
      .setN(2)
      .setInputCol("desc_filtered")
      .setOutputCol("desc_bigrams")
    
    // TF-IDF para unigrams
    val descHashingTF = new HashingTF()
      .setInputCol("desc_filtered")
      .setOutputCol("desc_tf")
      .setNumFeatures(200)
    
    val descIDF = new IDF()
      .setInputCol("desc_tf")
      .setOutputCol("description_features")
    
    // TF-IDF para bigrams
    val bigramHashingTF = new HashingTF()
      .setInputCol("desc_bigrams")
      .setOutputCol("bigram_tf")
      .setNumFeatures(100)
    
    val bigramIDF = new IDF()
      .setInputCol("bigram_tf")
      .setOutputCol("bigram_features")
    
    // 2. FeatureHasher para genre (cardinalidad reducida)
    val genreHasher = new FeatureHasher()
      .setInputCols(Array("genre"))
      .setOutputCol("genre_features")
      .setNumFeatures(64)
    
    // 3. StringIndexer para duration_category
    val durationIndexer = new StringIndexer()
      .setInputCol("duration_category")
      .setOutputCol("duration_indexed")
      .setHandleInvalid("keep")
    
    // 4. Assembler final con TODAS las features
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "description_features",    // 200 features (unigrams)
        "bigram_features",          // 100 features (bigrams)
        "genre_features",           // 64 features (genre hashed)
        "director_encoded",         // 1 feature (target encoded)
        "actors_encoded",           // 1 feature (target encoded)
        "duration",                 // 1 feature
        "duration_indexed",         // 1 feature
        "votes",                    // 1 feature
        "log_votes",                // 1 feature
        "reviews_from_users",       // 1 feature
        "reviews_from_critics",     // 1 feature
        "year_clean",               // 1 feature
        "decade",                   // 1 feature
        "votes_per_review",         // 1 feature
        "review_ratio",             // 1 feature
        "is_recent",                // 1 feature
        "is_old_classic"            // 1 feature
        // TOTAL: ~377 features
      ))
      .setOutputCol("features")
      .setHandleInvalid("skip")
    
    // 5. Scaler para normalizar features
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithStd(true)
      .setWithMean(false)  // Importante para sparse vectors
    
    // Pipeline completo
    new Pipeline().setStages(Array(
      descTokenizer, descRemover, descNGram,
      descHashingTF, descIDF,
      bigramHashingTF, bigramIDF,
      genreHasher,
      durationIndexer,
      assembler,
      scaler,
      regressor
    ))
  }
  
  // ==========================================================================
  // ENTRENAMIENTO DE MODELOS
  // ==========================================================================
  
  def entrenarModeloBaseline(trainData: DataFrame, testData: DataFrame): (PipelineModel, Map[String, Double], Double) = {
    println("   ⏳ Entrenando Ridge Regression (L2 regularization)...")
    
    val startTime = System.nanoTime()
    
    val lr = new LinearRegression()
      .setLabelCol("avg_vote")
      .setFeaturesCol("scaled_features")
      .setMaxIter(100)
      .setRegParam(0.1)        // L2 regularization
      .setElasticNetParam(0.0) // Pure Ridge
      .setTol(1e-6)
    
    val pipeline = crearPipelineAvanzado(lr)
    val model = pipeline.fit(trainData)
    
    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9
    
    val predictions = model.transform(testData)
    val metrics = evaluarModelo(predictions)
    
    (model, metrics, elapsedTime)
  }
  
  def entrenarRandomForestConCV(trainData: DataFrame, testData: DataFrame): (PipelineModel, Map[String, Double], Double) = {
    println("   ⏳ Configurando Grid Search para Random Forest...")
    
    val startTime = System.nanoTime()
    
    val rf = new RandomForestRegressor()
      .setLabelCol("avg_vote")
      .setFeaturesCol("scaled_features")
      .setSeed(42)
    
    val pipeline = crearPipelineAvanzado(rf)
    
    // Grid de hiperparámetros
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(50, 100, 150))
      .addGrid(rf.maxDepth, Array(8, 10, 12))
      .addGrid(rf.minInstancesPerNode, Array(5, 10))
      .build()
    
    println(s"   📊 Total de combinaciones: ${paramGrid.length}")
    
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator()
        .setLabelCol("avg_vote")
        .setPredictionCol("prediction")
        .setMetricName("rmse"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // 3-fold CV
      .setParallelism(4)
      .setSeed(42)
    
    println("   ⏳ Ejecutando Cross-Validation (esto tomará tiempo)...")
    val cvModel = cv.fit(trainData)
    
    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9
    
    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    val rfStage = bestModel.stages.last.asInstanceOf[org.apache.spark.ml.regression.RandomForestRegressionModel]
    
    println(s"\n   ✅ Mejores hiperparámetros encontrados:")
    println(s"      - numTrees: ${rfStage.getNumTrees}")
    println(s"      - maxDepth: ${rfStage.getMaxDepth}")
    println(s"      - minInstancesPerNode: ${rfStage.getMinInstancesPerNode}")
    
    val predictions = bestModel.transform(testData)
    val metrics = evaluarModelo(predictions)
    
    (bestModel, metrics, elapsedTime)
  }
  
  def entrenarGBTConCV(trainData: DataFrame, testData: DataFrame): (PipelineModel, Map[String, Double], Double) = {
    println("   ⏳ Configurando Grid Search para Gradient Boosted Trees...")
    
    val startTime = System.nanoTime()
    
    val gbt = new GBTRegressor()
      .setLabelCol("avg_vote")
      .setFeaturesCol("scaled_features")
      .setSeed(42)
    
    val pipeline = crearPipelineAvanzado(gbt)
    
    // Grid de hiperparámetros
    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxIter, Array(50, 100, 150))
      .addGrid(gbt.maxDepth, Array(4, 6, 8))
      .addGrid(gbt.stepSize, Array(0.05, 0.1, 0.2))
      .build()
    
    println(s"   📊 Total de combinaciones: ${paramGrid.length}")
    
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator()
        .setLabelCol("avg_vote")
        .setPredictionCol("prediction")
        .setMetricName("rmse"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // 3-fold CV
      .setParallelism(4)
      .setSeed(42)
    
    println("   ⏳ Ejecutando Cross-Validation (esto tomará tiempo)...")
    val cvModel = cv.fit(trainData)
    
    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9
    
    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    val gbtStage = bestModel.stages.last.asInstanceOf[org.apache.spark.ml.regression.GBTRegressionModel]
    
    println(s"\n   ✅ Mejores hiperparámetros encontrados:")
    println(s"      - maxIter: ${gbtStage.getNumTrees}")
    println(s"      - maxDepth: ${gbtStage.getMaxDepth}")
    println(s"      - stepSize: ${gbtStage.getStepSize}")
    
    val predictions = bestModel.transform(testData)
    val metrics = evaluarModelo(predictions)
    
    (bestModel, metrics, elapsedTime)
  }
  
  def crearEnsemble(
    baselineModel: PipelineModel,
    rfModel: PipelineModel,
    gbtModel: PipelineModel,
    testData: DataFrame
  ): Map[String, Double] = {
    
    println("   ⏳ Generando predicciones de modelos individuales...")
    
    val baselinePred = baselineModel.transform(testData)
      .select(col("avg_vote"), col("prediction").alias("pred_baseline"))
    
    val rfPred = rfModel.transform(testData)
      .select(col("prediction").alias("pred_rf"))
    
    val gbtPred = gbtModel.transform(testData)
      .select(col("prediction").alias("pred_gbt"))
    
    // Combinar predicciones
    val combined = baselinePred
      .withColumn("id", monotonically_increasing_id())
      .join(rfPred.withColumn("id", monotonically_increasing_id()), "id")
      .join(gbtPred.withColumn("id", monotonically_increasing_id()), "id")
    
    // Ensemble: 20% baseline + 30% RF + 50% GBT
    val ensemble = combined.withColumn(
      "prediction",
      col("pred_baseline") * 0.2 + col("pred_rf") * 0.3 + col("pred_gbt") * 0.5
    )
    
    println("   ✓ Ensemble creado (0.2*Baseline + 0.3*RF + 0.5*GBT)")
    
    // Guardar predicciones del ensemble
    guardarPredicciones(
      ensemble.select("avg_vote", "prediction"),
      "ml_prediction/resultados/advanced_ensemble_predictions.txt"
    )
    
    evaluarModelo(ensemble)
  }
  
  // ==========================================================================
  // EVALUACIÓN Y REPORTES
  // ==========================================================================
  
  def evaluarModelo(predictions: DataFrame): Map[String, Double] = {
    val evaluator = new RegressionEvaluator()
      .setLabelCol("avg_vote")
      .setPredictionCol("prediction")
    
    val rmse = evaluator.setMetricName("rmse").evaluate(predictions)
    val mae = evaluator.setMetricName("mae").evaluate(predictions)
    val r2 = evaluator.setMetricName("r2").evaluate(predictions)
    val mse = evaluator.setMetricName("mse").evaluate(predictions)
    
    Map("RMSE" -> rmse, "MAE" -> mae, "R2" -> r2, "MSE" -> mse)
  }
  
  def imprimirMetricas(modelName: String, metrics: Map[String, Double], time: Double): Unit = {
    println(s"\n   ✅ Métricas de $modelName:")
    println(f"      RMSE: ${metrics("RMSE")}%.4f")
    println(f"      MAE:  ${metrics("MAE")}%.4f")
    println(f"      R²:   ${metrics("R2")}%.4f")
    println(f"      MSE:  ${metrics("MSE")}%.4f")
    if (time > 0) {
      println(f"      Tiempo: ${time}%.2f segundos (${time/60}%.2f minutos)")
    }
  }
  
  def imprimirFeatureImportances(model: PipelineModel, modelName: String): Unit = {
    val treeModel = model.stages.last match {
      case rf: org.apache.spark.ml.regression.RandomForestRegressionModel => 
        Some(rf.featureImportances)
      case gbt: org.apache.spark.ml.regression.GBTRegressionModel => 
        Some(gbt.featureImportances)
      case _ => None
    }
    
    treeModel.foreach { importances =>
      println(s"\n   📊 Feature Importances (Top 15):")
      val topFeatures = importances.toArray.zipWithIndex
        .sortBy(-_._1)
        .take(15)
      
      topFeatures.foreach { case (importance, idx) =>
        println(f"      Feature $idx%3d: ${importance * 100}%.2f%%")
      }
    }
  }
  
  def guardarPredicciones(model: PipelineModel, testData: DataFrame, outputPath: String): Unit = {
    val predictions = model.transform(testData)
      .select("avg_vote", "prediction")
      .withColumn("error", abs(col("avg_vote") - col("prediction")))
      .orderBy(desc("error"))
      .limit(20)
    
    guardarPredicciones(predictions, outputPath)
  }
  
  def guardarPredicciones(predictions: DataFrame, outputPath: String): Unit = {
    val predList = predictions.collect()
    
    val writer = new PrintWriter(outputPath)
    writer.println("=" * 80)
    writer.println("TOP 20 PREDICCIONES CON MAYOR ERROR")
    writer.println("=" * 80)
    writer.println(f"${"Real"}%-10s ${"Predicción"}%-12s ${"Error"}%-10s")
    writer.println("-" * 80)
    
    predList.foreach { row =>
      val real = row.getDouble(0)
      val pred = row.getDouble(1)
      val error = math.abs(real - pred)
      writer.println(f"$real%-10.4f $pred%-12.4f $error%-10.4f")
    }
    
    writer.println("=" * 80)
    writer.close()
    
    println(s"   ✓ Predicciones guardadas en: $outputPath")
  }
  
  def generarReporteComparativoAvanzado(
    modelos: Map[String, (Map[String, Double], Double)]
  ): Unit = {
    val outputPath = "ml_prediction/resultados/reporte_avanzado.txt"
    val writer = new PrintWriter(outputPath)
    
    writer.println("=" * 80)
    writer.println("REPORTE COMPARATIVO AVANZADO - MODELOS DE PREDICCIÓN IMDB")
    writer.println("=" * 80)
    writer.println()
    
    writer.println("CONFIGURACIÓN:")
    writer.println("-" * 80)
    writer.println("• Features de Texto:")
    writer.println("  - Description: TF-IDF Unigrams (200 features)")
    writer.println("  - Description: TF-IDF Bigrams (100 features)")
    writer.println("• Features Categóricas:")
    writer.println("  - Genre: Feature Hashing (64 features)")
    writer.println("  - Director: Target Encoding (1 feature)")
    writer.println("  - Actors: Target Encoding (1 feature)")
    writer.println("  - Duration Category: StringIndexer (1 feature)")
    writer.println("• Features Numéricas (11 features):")
    writer.println("  - duration, votes, log_votes, reviews_from_users, reviews_from_critics")
    writer.println("  - year_clean, decade, votes_per_review, review_ratio")
    writer.println("  - is_recent, is_old_classic")
    writer.println("• TOTAL: ~378 features")
    writer.println("• Normalización: StandardScaler")
    writer.println("• Optimización: Grid Search con 3-fold Cross-Validation")
    writer.println()
    
    writer.println("RESULTADOS:")
    writer.println("=" * 80)
    writer.println(f"${"Modelo"}%-35s ${"RMSE"}%-10s ${"MAE"}%-10s ${"R²"}%-10s ${"Tiempo"}%-15s")
    writer.println("-" * 80)
    
    modelos.toSeq.sortBy(_._2._1("RMSE")).foreach { case (nombre, (metricas, tiempo)) =>
      val tiempoStr = if (tiempo > 0) f"${tiempo/60}%.2f min" else "N/A"
      writer.println(
        f"$nombre%-35s ${metricas("RMSE")}%-10.4f ${metricas("MAE")}%-10.4f ${metricas("R2")}%-10.4f $tiempoStr%-15s"
      )
    }
    
    writer.println("=" * 80)
    writer.println()
    
    // Encontrar mejor modelo
    val mejorModelo = modelos.minBy(_._2._1("RMSE"))
    val baselineMetrics = modelos("Ridge Regression")._1
    
    writer.println("ANÁLISIS:")
    writer.println("-" * 80)
    writer.println(s"🏆 Mejor modelo: ${mejorModelo._1}")
    writer.println(f"   - RMSE: ${mejorModelo._2._1("RMSE")}%.4f")
    writer.println(f"   - R²: ${mejorModelo._2._1("R2")}%.4f")
    writer.println()
    
    val mejoriaRMSE = ((baselineMetrics("RMSE") - mejorModelo._2._1("RMSE")) / baselineMetrics("RMSE") * 100)
    val mejoriaR2 = ((mejorModelo._2._1("R2") - baselineMetrics("R2")) / baselineMetrics("R2") * 100)
    
    writer.println(s"📈 Mejora sobre baseline:")
    writer.println(f"   - RMSE: $mejoriaRMSE%.2f%% mejor")
    writer.println(f"   - R²: $mejoriaR2%.2f%% mejor")
    writer.println()
    
    writer.println("INTERPRETACIÓN:")
    writer.println("-" * 80)
    val r2 = mejorModelo._2._1("R2")
    val interpretacion = if (r2 >= 0.5) {
      "Excelente - El modelo explica más de la mitad de la varianza"
    } else if (r2 >= 0.35) {
      "Bueno - El modelo captura patrones significativos"
    } else if (r2 >= 0.2) {
      "Moderado - Hay espacio para mejorar"
    } else {
      "Limitado - Se requieren más features o datos"
    }
    writer.println(s"• R² de ${r2}: $interpretacion")
    writer.println(s"• RMSE de ${mejorModelo._2._1("RMSE")}: Error promedio de ~${mejorModelo._2._1("RMSE")} puntos en escala 1-10")
    writer.println()
    
    writer.println("RECOMENDACIONES:")
    writer.println("-" * 80)
    writer.println("1. El modelo Ensemble combina las fortalezas de todos los modelos")
    writer.println("2. Features de texto (description) son las más importantes")
    writer.println("3. Target Encoding captura patrones de directores/actores exitosos")
    writer.println("4. Considerar agregar:")
    writer.println("   - Información de presupuesto/recaudación")
    writer.println("   - Premios y nominaciones")
    writer.println("   - Análisis de sentimiento en description")
    writer.println("   - Word2Vec para capturar semántica")
    writer.println()
    
    writer.println("=" * 80)
    writer.close()
    
    println(s"   ✓ Reporte guardado en: $outputPath")
    
    // Imprimir resumen en consola
    println("\n" + "=" * 80)
    println("📊 RESUMEN FINAL")
    println("=" * 80)
    println(f"${"Modelo"}%-35s ${"RMSE"}%-10s ${"R²"}%-10s")
    println("-" * 80)
    modelos.toSeq.sortBy(_._2._1("RMSE")).foreach { case (nombre, (metricas, _)) =>
      println(f"$nombre%-35s ${metricas("RMSE")}%-10.4f ${metricas("R2")}%-10.4f")
    }
    println("=" * 80)
    println(s"🏆 Mejor modelo: ${mejorModelo._1}")
    println(f"📈 Mejora sobre baseline: RMSE ${mejoriaRMSE}%.2f%% | R² ${mejoriaR2}%.2f%%")
    println("=" * 80)
  }
}

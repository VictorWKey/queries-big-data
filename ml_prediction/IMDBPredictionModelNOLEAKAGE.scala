import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor, GBTRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter

// :load ml_prediction/IMDBPredictionModelNOLEAKAGE.scala
// IMDBPredictionModelNOLEAKAGE.main(Array())

object IMDBPredictionModelNOLEAKAGE {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IMDB Rating Prediction - NO LEAKAGE")
      .master("local[*]")
      .config("spark.driver.memory", "10g")
      .config("spark.executor.memory", "10g")
      .config("spark.memory.fraction", "0.8")
      .config("spark.memory.storageFraction", "0.2")
      .config("spark.sql.shuffle.partitions", "100")
      .config("spark.default.parallelism", "100")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    println("=" * 80)
    println("🛡️  MODELO DE PREDICCIÓN - SIN DATA LEAKAGE - CALIFICACIÓN IMDB")
    println("=" * 80)
    println()
    println("⚠️  VARIABLES EXCLUIDAS (data leakage):")
    println("   ❌ votes - Correlacionado con popularidad del rating")
    println("   ❌ reviews_from_users - Consecuencia del rating")
    println("   ❌ reviews_from_critics - Consecuencia del rating")
    println("   ❌ Variables derivadas de las anteriores")
    println()
    println("✅ VARIABLES PERMITIDAS (predictores legítimos):")
    println("   ✓ description - Contenido independiente")
    println("   ✓ genre - Característica intrínseca")
    println("   ✓ director - Característica intrínseca")
    println("   ✓ actors - Característica intrínseca")
    println("   ✓ duration - Característica intrínseca")
    println("   ✓ year - Característica temporal")
    println("=" * 80)
    println()
    
    val moviesPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv"
    val ratingsPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb ratings.csv"
    
    println("📊 PASO 1: Cargando y preparando datos...")
    val fullDF = cargarYJoinearDatos(spark, moviesPath, ratingsPath)
    
    println("\n🧹 PASO 2: Limpiando datos (SIN usar votes/reviews)...")
    val cleanDF = limpiarDatosSinLeakage(fullDF)
    
    println("\n🔧 PASO 3: Dividiendo train/test (80/20)...")
    val Array(trainDataRaw, testDataRaw) = cleanDF.randomSplit(Array(0.8, 0.2), seed = 42)
    
    println("\n🎯 PASO 4: Aplicando Feature Engineering SOLO en train...")
    val (trainData, testData, encodingMaps) = aplicarFeatureEngineeringConSplit(trainDataRaw, testDataRaw)
    
    val trainOptimized = trainData.repartition(100).persist(StorageLevel.MEMORY_AND_DISK)
    val testOptimized = testData.repartition(100).persist(StorageLevel.MEMORY_AND_DISK)
    
    val trainCount = trainOptimized.count()
    val testCount = testOptimized.count()
    
    println(s"   ✓ Datos de entrenamiento: $trainCount filas")
    println(s"   ✓ Datos de prueba: $testCount filas")
    println(s"   ✓ Director encodings: ${encodingMaps("director").size} categorías")
    println(s"   ✓ Actors encodings: ${encodingMaps("actors").size} categorías")
    
    // ============================================================================
    // MODELO 1: Ridge Regression (Baseline)
    // ============================================================================
    println("\n📈 PASO 5: Entrenando BASELINE (Ridge Regression)...")
    val (baselineModel, baselineMetrics, baselineTime) = entrenarModeloBaseline(
      trainOptimized, testOptimized
    )
    imprimirMetricas("Ridge Regression (Baseline)", baselineMetrics, baselineTime)
    guardarPredicciones(
      baselineModel, 
      testOptimized, 
      "ml_prediction/resultados/noleakage_baseline_predictions.txt"
    )
    
    // ============================================================================
    // MODELO 2: Random Forest
    // ============================================================================
    println("\n🌲 PASO 6: Entrenando Random Forest...")
    val (rfModel, rfMetrics, rfTime) = entrenarRandomForest(
      trainOptimized, testOptimized
    )
    imprimirMetricas("Random Forest", rfMetrics, rfTime)
    imprimirFeatureImportances(rfModel, "Random Forest")
    guardarPredicciones(
      rfModel, 
      testOptimized, 
      "ml_prediction/resultados/noleakage_rf_predictions.txt"
    )
    
    // ============================================================================
    // MODELO 3: Gradient Boosted Trees
    // ============================================================================
    println("\n🚀 PASO 7: Entrenando GBT...")
    val (gbtModel, gbtMetrics, gbtTime) = entrenarGBT(
      trainOptimized, testOptimized
    )
    imprimirMetricas("Gradient Boosted Trees", gbtMetrics, gbtTime)
    imprimirFeatureImportances(gbtModel, "GBT")
    guardarPredicciones(
      gbtModel, 
      testOptimized, 
      "ml_prediction/resultados/noleakage_gbt_predictions.txt"
    )
    
    // ============================================================================
    // MODELO 4: Ensemble
    // ============================================================================
    println("\n🎭 PASO 8: Creando modelo ENSEMBLE...")
    val ensembleMetrics = crearEnsemble(
      baselineModel, rfModel, gbtModel, testOptimized
    )
    imprimirMetricas("Ensemble (0.2*Ridge + 0.3*RF + 0.5*GBT)", ensembleMetrics, 0.0)
    
    // ============================================================================
    // REPORTE FINAL
    // ============================================================================
    println("\n📊 PASO 9: Generando reporte comparativo...")
    generarReporteComparativo(
      Map(
        "Ridge Regression" -> (baselineMetrics, baselineTime),
        "Random Forest" -> (rfMetrics, rfTime),
        "Gradient Boosted Trees" -> (gbtMetrics, gbtTime),
        "Ensemble" -> (ensembleMetrics, 0.0)
      )
    )
    
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
  
  def limpiarDatosSinLeakage(df: DataFrame): DataFrame = {
    val originalCount = df.count()
    
    val dfYearCleaned = df.withColumn(
      "year_clean",
      regexp_replace(col("year"), "[^0-9]", "").cast(IntegerType)
    )
    
    // CRÍTICO: NO usar votes, reviews_from_users, reviews_from_critics
    val cleanDF = dfYearCleaned.na.drop(Seq(
      "description", "genre", "director", "actors",
      "duration", "avg_vote", "year_clean"
    ))
    
    val cleanCount = cleanDF.count()
    val lossPercent = ((originalCount - cleanCount).toDouble / originalCount * 100)
    
    println(s"   ✓ Filas originales: $originalCount")
    println(s"   ✓ Filas limpias: $cleanCount")
    println(f"   ✓ Pérdida: $lossPercent%.2f%%")
    
    cleanDF
  }
  
  def aplicarFeatureEngineeringConSplit(
    trainRaw: DataFrame, 
    testRaw: DataFrame
  ): (DataFrame, DataFrame, Map[String, Map[String, Double]]) = {
    
    println("   🔧 Aplicando Target Encoding para director (SOLO TRAIN)...")
    val (trainWithDirector, directorMap) = targetEncodeOnTrain(trainRaw, "director", "avg_vote")
    val testWithDirector = applyTargetEncoding(testRaw, "director", directorMap)
    
    println("   🔧 Aplicando Target Encoding para actors (SOLO TRAIN)...")
    val (trainWithActors, actorsMap) = targetEncodeOnTrain(trainWithDirector, "actors", "avg_vote")
    val testWithActors = applyTargetEncoding(testWithDirector, "actors", actorsMap)
    
    println("   🔧 Reduciendo cardinalidad de genre (basado en TRAIN)...")
    val (trainWithGenre, genreTopCategories) = reduceCardinalityOnTrain(trainWithActors, "genre", topN = 30)
    val testWithGenre = applyCardinalityReduction(testWithActors, "genre", genreTopCategories)
    
    println("   🔧 Creando features derivadas (SIN LEAKAGE)...")
    val trainEnriched = crearFeaturesDerivadasSinLeakage(trainWithGenre)
    val testEnriched = crearFeaturesDerivadasSinLeakage(testWithGenre)
    
    println("   ✓ Feature Engineering completado SIN data leakage")
    
    val encodingMaps = Map(
      "director" -> directorMap,
      "actors" -> actorsMap
    )
    
    (trainEnriched, testEnriched, encodingMaps)
  }
  
  def targetEncodeOnTrain(
    df: DataFrame, 
    column: String, 
    targetCol: String
  ): (DataFrame, Map[String, Double]) = {
    
    val globalMean = df.select(mean(targetCol)).first().getDouble(0)
    
    val avgByCategory = df.groupBy(column)
      .agg(
        mean(targetCol).alias("category_mean"),
        count("*").alias("category_count")
      )
      .collect()
    
    val smoothingFactor = 10.0
    val encodingMap = avgByCategory.map { row =>
      val category = row.getString(0)
      val categoryMean = row.getDouble(1)
      val categoryCount = row.getLong(2)
      val smoothedValue = (categoryMean * categoryCount + globalMean * smoothingFactor) / 
                          (categoryCount + smoothingFactor)
      category -> smoothedValue
    }.toMap
    
    val encodingMapWithDefault = encodingMap + ("__UNKNOWN__" -> globalMean)
    
    val dfEncoded = applyTargetEncoding(df, column, encodingMapWithDefault)
    
    (dfEncoded, encodingMapWithDefault)
  }
  
  def applyTargetEncoding(
    df: DataFrame, 
    column: String, 
    encodingMap: Map[String, Double]
  ): DataFrame = {
    
    val defaultValue = encodingMap.getOrElse("__UNKNOWN__", 0.0)
    
    val encodeUDF = udf((value: String) => 
      encodingMap.getOrElse(value, defaultValue)
    )
    
    df.withColumn(s"${column}_encoded", encodeUDF(col(column)))
      .drop(column)
  }
  
  def reduceCardinalityOnTrain(
    df: DataFrame, 
    column: String, 
    topN: Int
  ): (DataFrame, Set[String]) = {
    
    val topCategories = df.groupBy(column)
      .count()
      .orderBy(desc("count"))
      .limit(topN)
      .select(column)
      .collect()
      .map(_.getString(0))
      .toSet
    
    val dfReduced = applyCardinalityReduction(df, column, topCategories)
    
    (dfReduced, topCategories)
  }
  
  def applyCardinalityReduction(
    df: DataFrame, 
    column: String, 
    topCategories: Set[String]
  ): DataFrame = {
    
    val categorizeUDF = udf((value: String) => 
      if (topCategories.contains(value)) value else "Other"
    )
    
    df.withColumn(column, categorizeUDF(col(column)))
  }
  
  // CRÍTICO: Features derivadas SIN usar votes/reviews
  def crearFeaturesDerivadasSinLeakage(df: DataFrame): DataFrame = {
    df
      .withColumn("decade", 
        (col("year_clean") / 10).cast(IntegerType) * 10)
      .withColumn("is_recent", 
        when(col("year_clean") >= 2015, 1.0).otherwise(0.0))
      .withColumn("is_old_classic", 
        when(col("year_clean") <= 1980, 1.0).otherwise(0.0))
      .withColumn("duration_category",
        when(col("duration") <= 90, "short")
        .when(col("duration") <= 120, "medium")
        .otherwise("long"))
  }
  
  // ==========================================================================
  // CREACIÓN DE PIPELINES
  // ==========================================================================
  
  def crearPipeline(regressor: org.apache.spark.ml.Estimator[_]): Pipeline = {
    val descTokenizer = new RegexTokenizer()
      .setInputCol("description")
      .setOutputCol("desc_words")
      .setPattern("\\W+")
      .setMinTokenLength(3)
    
    val descRemover = new StopWordsRemover()
      .setInputCol("desc_words")
      .setOutputCol("desc_filtered")
    
    val descHashingTF = new HashingTF()
      .setInputCol("desc_filtered")
      .setOutputCol("desc_tf")
      .setNumFeatures(100)
    
    val descIDF = new IDF()
      .setInputCol("desc_tf")
      .setOutputCol("description_features")
    
    val genreHasher = new FeatureHasher()
      .setInputCols(Array("genre"))
      .setOutputCol("genre_features")
      .setNumFeatures(16)
    
    val durationIndexer = new StringIndexer()
      .setInputCol("duration_category")
      .setOutputCol("duration_indexed")
      .setHandleInvalid("keep")
    
    // CRÍTICO: SOLO features sin leakage (TOTAL: ~122 features)
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "description_features",    // 100 features
        "genre_features",           // 16 features
        "director_encoded",         // 1 feature
        "actors_encoded",           // 1 feature
        "duration",                 // 1 feature
        "duration_indexed",         // 1 feature
        "year_clean",               // 1 feature
        "decade",                   // 1 feature
        "is_recent",                // 1 feature
        "is_old_classic"            // 1 feature
        // ❌ NO: votes, log_votes, reviews_from_users, reviews_from_critics
        // ❌ NO: votes_per_review, review_ratio
      ))
      .setOutputCol("features")
      .setHandleInvalid("skip")
    
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithStd(true)
      .setWithMean(false)
    
    new Pipeline().setStages(Array(
      descTokenizer, descRemover,
      descHashingTF, descIDF,
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
    println("   ⏳ Entrenando Ridge Regression...")
    
    val startTime = System.nanoTime()
    
    val lr = new LinearRegression()
      .setLabelCol("avg_vote")
      .setFeaturesCol("scaled_features")
      .setMaxIter(100)
      .setRegParam(0.1)
      .setElasticNetParam(0.0)
      .setTol(1e-6)
    
    val pipeline = crearPipeline(lr)
    val model = pipeline.fit(trainData)
    
    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9
    
    val predictions = model.transform(testData)
    val metrics = evaluarModelo(predictions)
    
    (model, metrics, elapsedTime)
  }
  
  def entrenarRandomForest(trainData: DataFrame, testData: DataFrame): (PipelineModel, Map[String, Double], Double) = {
    println("   ⏳ Entrenando Random Forest...")
    
    val startTime = System.nanoTime()
    
    val rf = new RandomForestRegressor()
      .setLabelCol("avg_vote")
      .setFeaturesCol("scaled_features")
      .setNumTrees(30)
      .setMaxDepth(8)
      .setMinInstancesPerNode(10)
      .setSubsamplingRate(0.8)
      .setSeed(42)
    
    val pipeline = crearPipeline(rf)
    val model = pipeline.fit(trainData)
    
    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9
    
    val predictions = model.transform(testData)
    val metrics = evaluarModelo(predictions)
    
    (model, metrics, elapsedTime)
  }
  
  def entrenarGBT(trainData: DataFrame, testData: DataFrame): (PipelineModel, Map[String, Double], Double) = {
    println("   ⏳ Entrenando Gradient Boosted Trees...")
    
    val startTime = System.nanoTime()
    
    val gbt = new GBTRegressor()
      .setLabelCol("avg_vote")
      .setFeaturesCol("scaled_features")
      .setMaxIter(50)
      .setMaxDepth(5)
      .setStepSize(0.1)
      .setSubsamplingRate(0.8)
      .setSeed(42)
    
    val pipeline = crearPipeline(gbt)
    val model = pipeline.fit(trainData)
    
    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9
    
    val predictions = model.transform(testData)
    val metrics = evaluarModelo(predictions)
    
    (model, metrics, elapsedTime)
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
    
    val combined = baselinePred
      .withColumn("id", monotonically_increasing_id())
      .join(rfPred.withColumn("id", monotonically_increasing_id()), "id")
      .join(gbtPred.withColumn("id", monotonically_increasing_id()), "id")
    
    val ensemble = combined.withColumn(
      "prediction",
      col("pred_baseline") * 0.2 + col("pred_rf") * 0.3 + col("pred_gbt") * 0.5
    )
    
    println("   ✓ Ensemble creado (0.2*Baseline + 0.3*RF + 0.5*GBT)")
    
    guardarPredicciones(
      ensemble.select("avg_vote", "prediction"),
      "ml_prediction/resultados/noleakage_ensemble_predictions.txt"
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
      println(s"\n   📊 Feature Importances (Top 10):")
      val topFeatures = importances.toArray.zipWithIndex
        .sortBy(-_._1)
        .take(10)
      
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
  
  def generarReporteComparativo(modelos: Map[String, (Map[String, Double], Double)]): Unit = {
    val outputPath = "ml_prediction/resultados/reporte_noleakage.txt"
    val writer = new PrintWriter(outputPath)
    
    writer.println("=" * 80)
    writer.println("🛡️  REPORTE - MODELOS SIN DATA LEAKAGE")
    writer.println("=" * 80)
    writer.println()
    
    writer.println("CONFIGURACIÓN:")
    writer.println("-" * 80)
    writer.println("⚠️  VARIABLES EXCLUIDAS (data leakage):")
    writer.println("   ❌ votes - Correlacionado con popularidad del rating")
    writer.println("   ❌ log_votes - Derivado de votes")
    writer.println("   ❌ reviews_from_users - Consecuencia del rating")
    writer.println("   ❌ reviews_from_critics - Consecuencia del rating")
    writer.println("   ❌ votes_per_review - Derivado de votes/reviews")
    writer.println("   ❌ review_ratio - Derivado de reviews")
    writer.println()
    writer.println("✅ FEATURES UTILIZADAS (predictores legítimos):")
    writer.println("  • Description: TF-IDF Unigrams (100 features)")
    writer.println("  • Genre: Feature Hashing (16 features) - Top 30 categorías")
    writer.println("  • Director: Target Encoding (1 feature)")
    writer.println("  • Actors: Target Encoding (1 feature)")
    writer.println("  • Duration: Numérica (1 feature)")
    writer.println("  • Duration Category: StringIndexer (1 feature)")
    writer.println("  • Year: Numérica (1 feature)")
    writer.println("  • Decade: Categórica (1 feature)")
    writer.println("  • Is Recent: Binaria (1 feature)")
    writer.println("  • Is Old Classic: Binaria (1 feature)")
    writer.println("  • TOTAL: ~122 features")
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
    
    val mejorModelo = modelos.minBy(_._2._1("RMSE"))
    val baselineMetrics = modelos("Ridge Regression")._1
    
    writer.println("ANÁLISIS:")
    writer.println("-" * 80)
    writer.println(s"🏆 Mejor modelo: ${mejorModelo._1}")
    writer.println(f"   - RMSE: ${mejorModelo._2._1("RMSE")}%.4f")
    writer.println(f"   - R²: ${mejorModelo._2._1("R2")}%.4f")
    writer.println()
    
    writer.println("📊 INTERPRETACIÓN DEL R²:")
    val r2 = mejorModelo._2._1("R2")
    if (r2 < 0.3) {
      writer.println("   ⚠️  R² BAJO (< 0.3): El modelo tiene capacidad predictiva limitada.")
      writer.println("   → Esto es ESPERADO cuando NO usamos variables con data leakage.")
    } else if (r2 < 0.5) {
      writer.println("   ✅ R² MODERADO (0.3-0.5): Capacidad predictiva aceptable.")
    } else if (r2 < 0.7) {
      writer.println("   ✅ R² BUENO (0.5-0.7): Buena capacidad predictiva.")
    } else {
      writer.println("   ⚠️  R² ALTO (> 0.7): Verificar posible data leakage residual.")
    }
    writer.println()
    
    val mejoriaRMSE = ((baselineMetrics("RMSE") - mejorModelo._2._1("RMSE")) / baselineMetrics("RMSE") * 100)
    val mejoriaR2 = ((mejorModelo._2._1("R2") - baselineMetrics("R2")) / baselineMetrics("R2") * 100)
    
    writer.println(s"📈 Mejora sobre baseline:")
    writer.println(f"   - RMSE: $mejoriaRMSE%.2f%% mejor")
    writer.println(f"   - R²: $mejoriaR2%.2f%% mejor")
    writer.println()
    
    writer.println("CONCLUSIONES:")
    writer.println("-" * 80)
    writer.println("✅ Este modelo es REALISTA y sin data leakage")
    writer.println("✅ Predice ratings basándose SOLO en características intrínsecas")
    writer.println("✅ Los ratings de IMDB son difíciles de predecir (subjetividad)")
    writer.println("⚠️  Un R² bajo NO significa mal modelo - significa honestidad científica")
    writer.println()
    
    writer.println("=" * 80)
    writer.close()
    
    println(s"   ✓ Reporte guardado en: $outputPath")
    
    println("\n" + "=" * 80)
    println("📊 RESUMEN FINAL (SIN DATA LEAKAGE)")
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

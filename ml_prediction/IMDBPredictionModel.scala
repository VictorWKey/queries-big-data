import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor, GBTRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import java.io.PrintWriter

// Cargar en Spark Shell:
// :load ml_prediction/IMDBPredictionModel.scala
// IMDBPredictionModel.main(Array())

object IMDBPredictionModel {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IMDB Rating Prediction - SparkML")
      .master("local[*]")
      .config("spark.driver.memory", "6g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    println("=" * 80)
    println("MODELO DE PREDICCIÓN DE CALIFICACIÓN IMDB - SPARKML")
    println("=" * 80)
    println()
    
    // Paths de los datos
    val moviesPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv"
    val ratingsPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb ratings.csv"
    
    // Paso 1: Cargar y preparar datos
    println("📊 PASO 1: Cargando y preparando datos...")
    val fullDF = cargarYJoinearDatos(spark, moviesPath, ratingsPath)
    
    // Paso 2: Limpiar y filtrar datos
    println("\n🧹 PASO 2: Limpiando datos (eliminando nulos en columnas clave)...")
    val cleanDF = limpiarDatos(fullDF)
    
    // Paso 3: Feature Engineering
    println("\n🔧 PASO 3: Procesando features textuales y numéricas...")
    val (trainData, testData) = prepararFeatures(cleanDF)
    
    // Paso 4: Modelo Baseline (Linear Regression)
    println("\n📈 PASO 4: Entrenando modelo BASELINE (Linear Regression)...")
    val baselineMetrics = entrenarModeloBaseline(trainData, testData, spark)
    
    // Paso 5: Modelo Principal (Random Forest)
    println("\n🌲 PASO 5: Entrenando modelo PRINCIPAL (Random Forest)...")
    val rfMetrics = entrenarRandomForest(trainData, testData, spark)
    
    // Paso 6: Modelo Adicional (Gradient Boosted Trees)
    println("\n🚀 PASO 6: Entrenando modelo ADICIONAL (Gradient Boosted Trees)...")
    val gbtMetrics = entrenarGradientBoosting(trainData, testData, spark)
    
    // Paso 7: Comparación y Reporte Final
    println("\n📊 PASO 7: Generando reporte comparativo...")
    generarReporteComparativo(baselineMetrics, rfMetrics, gbtMetrics, spark)
    
    println("\n" + "=" * 80)
    println("✅ PROCESO COMPLETADO - Resultados guardados en ml_prediction/resultados/")
    println("=" * 80)
    
    spark.stop()
  }
  
  // ============================================================================
  // FUNCIONES DE CARGA Y PREPARACIÓN DE DATOS
  // ============================================================================
  
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
    
    // Join de ambos datasets
    val fullDF = moviesDF.join(ratingsDF, Seq("imdb_title_id"), "left")
    
    println(s"   ✓ Películas cargadas: ${moviesDF.count()}")
    println(s"   ✓ Ratings cargados: ${ratingsDF.count()}")
    println(s"   ✓ Dataset completo: ${fullDF.count()} filas, ${fullDF.columns.length} columnas")
    
    fullDF
  }
  
  def limpiarDatos(df: DataFrame): DataFrame = {
    val originalCount = df.count()
    
    // Seleccionar solo las columnas que vamos a usar
    val selectedDF = df.select(
      col("imdb_title_id"),
      col("title"),
      col("year"),
      col("genre"),
      col("duration"),
      col("director"),
      col("writer"),
      col("production_company"),
      col("actors"),
      col("description"),
      col("avg_vote").as("label"), // Esta es nuestra variable objetivo
      col("votes"),
      col("reviews_from_users"),
      col("reviews_from_critics")
    )
    
    // Limpiar columna 'year' - convertir a numérico
    val withCleanYear = selectedDF.withColumn(
      "year_clean",
      when(col("year").cast("int").isNotNull, col("year").cast("int"))
        .otherwise(lit(null))
    ).drop("year")
    
    // Eliminar filas con nulos en columnas críticas
    val cleanDF = withCleanYear
      .na.drop(Seq(
        "label",          // avg_vote
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
      // Eliminar filas con strings vacíos en columnas textuales
      .filter(
        col("genre") =!= "" &&
        col("director") =!= "" &&
        col("actors") =!= "" &&
        col("description") =!= ""
      )
    
    val finalCount = cleanDF.count()
    val percentLost = ((originalCount - finalCount).toDouble / originalCount * 100)
    
    println(s"   ✓ Filas originales: $originalCount")
    println(s"   ✓ Filas después de limpieza: $finalCount")
    println(s"   ✓ Filas eliminadas: ${originalCount - finalCount} (${f"$percentLost%.2f"}%%)")
    
    cleanDF
  }
  
  // ============================================================================
  // FEATURE ENGINEERING
  // ============================================================================
  
  def prepararFeatures(df: DataFrame): (DataFrame, DataFrame) = {
    // Dividir en train y test (80/20)
    val Array(trainData, testData) = df.randomSplit(Array(0.8, 0.2), seed = 42)
    
    println(s"   ✓ Datos de entrenamiento: ${trainData.count()} filas")
    println(s"   ✓ Datos de prueba: ${testData.count()} filas")
    println(s"   ✓ Proporción: ${f"${trainData.count().toDouble / df.count() * 100}%.1f"}%% train / ${f"${testData.count().toDouble / df.count() * 100}%.1f"}%% test")
    
    (trainData, testData)
  }
  
  // ============================================================================
  // PIPELINE DE FEATURES
  // ============================================================================
  
  def crearPipelineFeatures(): Pipeline = {
    // 1. Procesar DESCRIPTION (texto largo) con TF-IDF
    val descTokenizer = new Tokenizer()
      .setInputCol("description")
      .setOutputCol("description_words")
    
    val descRemover = new StopWordsRemover()
      .setInputCol("description_words")
      .setOutputCol("description_filtered")
    
    val descHashingTF = new HashingTF()
      .setInputCol("description_filtered")
      .setOutputCol("description_tf")
      .setNumFeatures(100)
    
    val descIDF = new IDF()
      .setInputCol("description_tf")
      .setOutputCol("description_features")
    
    // 2. Procesar GENRE (convertir a índices)
    val genreIndexer = new StringIndexer()
      .setInputCol("genre")
      .setOutputCol("genre_index")
      .setHandleInvalid("keep")
    
    val genreEncoder = new OneHotEncoder()
      .setInputCol("genre_index")
      .setOutputCol("genre_vec")
    
    // 3. Procesar DIRECTOR
    val directorIndexer = new StringIndexer()
      .setInputCol("director")
      .setOutputCol("director_index")
      .setHandleInvalid("keep")
    
    val directorEncoder = new OneHotEncoder()
      .setInputCol("director_index")
      .setOutputCol("director_vec")
    
    // 4. Procesar ACTORS
    val actorsIndexer = new StringIndexer()
      .setInputCol("actors")
      .setOutputCol("actors_index")
      .setHandleInvalid("keep")
    
    val actorsEncoder = new OneHotEncoder()
      .setInputCol("actors_index")
      .setOutputCol("actors_vec")
    
    // 5. Procesar WRITER (opcional, puede tener nulos)
    val writerIndexer = new StringIndexer()
      .setInputCol("writer")
      .setOutputCol("writer_index")
      .setHandleInvalid("keep")
    
    val writerEncoder = new OneHotEncoder()
      .setInputCol("writer_index")
      .setOutputCol("writer_vec")
    
    // 6. Procesar PRODUCTION_COMPANY (opcional)
    val prodIndexer = new StringIndexer()
      .setInputCol("production_company")
      .setOutputCol("production_index")
      .setHandleInvalid("keep")
    
    val prodEncoder = new OneHotEncoder()
      .setInputCol("production_index")
      .setOutputCol("production_vec")
    
    // 7. Ensamblar todas las features
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "description_features",
        "genre_vec",
        "director_vec",
        "actors_vec",
        // Features numéricas
        "duration",
        "votes",
        "reviews_from_users",
        "reviews_from_critics",
        "year_clean"
      ))
      .setOutputCol("features")
      .setHandleInvalid("skip")
    
    // Crear pipeline con todas las etapas
    new Pipeline().setStages(Array(
      descTokenizer,
      descRemover,
      descHashingTF,
      descIDF,
      genreIndexer,
      genreEncoder,
      directorIndexer,
      directorEncoder,
      actorsIndexer,
      actorsEncoder,
      assembler
    ))
  }
  
  // ============================================================================
  // MODELOS
  // ============================================================================
  
  def entrenarModeloBaseline(trainData: DataFrame, testData: DataFrame, spark: SparkSession): Map[String, Double] = {
    // Pipeline de features + Linear Regression
    val featurePipeline = crearPipelineFeatures()
    val lr = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)
    
    val pipeline = new Pipeline().setStages(featurePipeline.getStages ++ Array(lr))
    
    println("   ⏳ Entrenando Linear Regression...")
    val startTime = System.currentTimeMillis()
    val model = pipeline.fit(trainData)
    val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    val predictions = model.transform(testData)
    
    val metrics = evaluarModelo(predictions, "Linear Regression (Baseline)", trainingTime)
    
    // Guardar predicciones de muestra
    guardarPrediccionesMuestra(predictions, "ml_prediction/resultados/baseline_predictions.txt")
    
    metrics
  }
  
  def entrenarRandomForest(trainData: DataFrame, testData: DataFrame, spark: SparkSession): Map[String, Double] = {
    val featurePipeline = crearPipelineFeatures()
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(50)
      .setMaxDepth(10)
      .setSeed(42)
    
    val pipeline = new Pipeline().setStages(featurePipeline.getStages ++ Array(rf))
    
    println("   ⏳ Entrenando Random Forest (50 árboles, profundidad 10)...")
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
  
  def entrenarGradientBoosting(trainData: DataFrame, testData: DataFrame, spark: SparkSession): Map[String, Double] = {
    val featurePipeline = crearPipelineFeatures()
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(30)
      .setMaxDepth(5)
      .setSeed(42)
    
    val pipeline = new Pipeline().setStages(featurePipeline.getStages ++ Array(gbt))
    
    println("   ⏳ Entrenando Gradient Boosted Trees (30 iteraciones, profundidad 5)...")
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
  
  // ============================================================================
  // EVALUACIÓN
  // ============================================================================
  
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
    println(f"      Tiempo de entrenamiento: $trainingTime%.2f segundos")
    
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
  
  // ============================================================================
  // REPORTE COMPARATIVO
  // ============================================================================
  
  def generarReporteComparativo(
    baselineMetrics: Map[String, Double],
    rfMetrics: Map[String, Double],
    gbtMetrics: Map[String, Double],
    spark: SparkSession
  ): Unit = {
    
    val outputPath = "ml_prediction/resultados/reporte_comparativo.txt"
    val writer = new PrintWriter(outputPath)
    
    writer.println("=" * 80)
    writer.println("REPORTE COMPARATIVO DE MODELOS - PREDICCIÓN DE RATING IMDB")
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
    writer.println(f"  - Mejora R²: ${(rfMetrics("r2") - baselineMetrics("r2")) * 100}%.2f puntos porcentuales")
    
    writer.println()
    writer.println(f"Gradient Boosted Trees vs Baseline:")
    writer.println(f"  - Reducción RMSE: $gbtImprovement%.2f%%")
    writer.println(f"  - Mejora R²: ${(gbtMetrics("r2") - baselineMetrics("r2")) * 100}%.2f puntos porcentuales")
    
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
    
    writer.println()
    writer.println("INTERPRETACIÓN")
    writer.println("-" * 80)
    writer.println("- Un R² más cercano a 1.0 indica mejor ajuste del modelo")
    writer.println("- Un RMSE más bajo indica mejores predicciones")
    writer.println("- MAE representa el error absoluto promedio en la escala de ratings (1-10)")
    
    writer.close()
    
    println(s"\n   ✓ Reporte guardado en: $outputPath")
    
    // También imprimir en consola
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

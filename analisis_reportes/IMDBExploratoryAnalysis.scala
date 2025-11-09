import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import java.io.PrintWriter
import java.io.File

// :load IMDBExploratoryAnalysis.scala
// IMDBExploratoryAnalysis.main(Array())
//
// TIPS PARA MEJOR VISUALIZACIÓN:
// 1. Las tablas anchas se guardan automáticamente en archivos .txt
// 2. Usa 'less -S archivo.txt' para scroll horizontal
// 3. O abre los archivos generados en VS Code

object IMDBExploratoryAnalysis {
   
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IMDB Movies - Exploratory Data Analysis")
      .master("local[*]")
      .config("spark.driver.memory", "6g")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    // Cargar los datasets
    val moviesPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv"
    val ratingsPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb ratings.csv"
    
    println("="*80)
    println("ANÁLISIS EXPLORATORIO - IMDB MOVIES DATASET")
    println("="*80)
    
    // Cargar movies
    val moviesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("escape", "\"")
      .csv(moviesPath)
    
    // Cargar ratings
    val ratingsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ratingsPath)
    
    // Join de los datasets
    val fullDF = moviesDF.join(ratingsDF, Seq("imdb_title_id"), "left")
    
    // Realizar análisis exploratorio
    analisisExploratorio(spark, moviesDF, ratingsDF, fullDF)
    
    spark.stop()
  }
  
  def analisisExploratorio(spark: SparkSession, moviesDF: DataFrame, ratingsDF: DataFrame, fullDF: DataFrame): Unit = {
    
    // Crear directorio para reportes si no existe
    val reportDir = new File("analisis_reportes")
    if (!reportDir.exists()) reportDir.mkdir()
    
    // ============================================================================
    // 1. INFORMACIÓN BÁSICA DEL DATASET MOVIES
    // ============================================================================
    println("\n" + "="*80)
    println("1. DATASET: IMDB MOVIES")
    println("="*80)
    
    println("\n--- Dimensiones del dataset ---")
    val moviesCount = moviesDF.count()
    val moviesColumns = moviesDF.columns.length
    println(s"Total de filas: $moviesCount")
    println(s"Total de columnas: $moviesColumns")
    
    println("\n--- Columnas disponibles ---")
    moviesDF.columns.zipWithIndex.foreach { case (col, idx) =>
      println(f"${idx + 1}%2d. $col")
    }
    
    println("\n--- Tipos de datos ---")
    moviesDF.schema.foreach { field =>
      println(f"${field.name}%-30s -> ${field.dataType}")
    }
    
    println("\n--- Valores nulos por columna ---")
    val moviesNullCounts = moviesDF.columns.map { colName =>
      (colName, moviesDF.filter(col(colName).isNull || col(colName) === "").count())
    }
    moviesNullCounts.foreach { case (colName, nullCount) =>
      val percentage = (nullCount.toDouble / moviesCount * 100)
      println(f"${colName}%-30s: ${nullCount}%6d nulos (${percentage}%5.2f%%)")
    }
    
    println("\n--- Muestra de datos (primeras 5 filas) ---")
    println(s"Mostrando las ${moviesDF.columns.length} columnas completas...")
    mostrarTablaLegible(moviesDF, 5, "movies_muestra", reportDir)
    
    println("\n--- Estadísticas descriptivas (columnas numéricas) ---")
    mostrarTablaLegible(moviesDF.select("year", "duration").describe(), 100, "movies_stats", reportDir)
    
    // ============================================================================
    // 2. INFORMACIÓN BÁSICA DEL DATASET RATINGS
    // ============================================================================
    println("\n" + "="*80)
    println("2. DATASET: IMDB RATINGS")
    println("="*80)
    
    println("\n--- Dimensiones del dataset ---")
    val ratingsCount = ratingsDF.count()
    val ratingsColumns = ratingsDF.columns.length
    println(s"Total de filas: $ratingsCount")
    println(s"Total de columnas: $ratingsColumns")
    
    println("\n--- Columnas disponibles ---")
    ratingsDF.columns.zipWithIndex.foreach { case (col, idx) =>
      println(f"${idx + 1}%2d. $col")
    }
    
    println("\n--- Tipos de datos ---")
    ratingsDF.schema.foreach { field =>
      println(f"${field.name}%-30s -> ${field.dataType}")
    }
    
    println("\n--- Valores nulos por columna ---")
    val ratingsNullCounts = ratingsDF.columns.map { colName =>
      (colName, ratingsDF.filter(col(colName).isNull || col(colName) === "").count())
    }
    ratingsNullCounts.foreach { case (colName, nullCount) =>
      val percentage = (nullCount.toDouble / ratingsCount * 100)
      println(f"${colName}%-30s: ${nullCount}%6d nulos (${percentage}%5.2f%%)")
    }
    
    println("\n--- Muestra de datos (primeras 5 filas) ---")
    println(s"Mostrando las ${ratingsDF.columns.length} columnas completas...")
    mostrarTablaLegible(ratingsDF, 5, "ratings_muestra", reportDir)
    
    println("\n--- Estadísticas descriptivas (columnas numéricas) ---")
    mostrarTablaLegible(ratingsDF.select("weighted_average_vote", "total_votes", "mean_vote", "median_vote").describe(), 100, "ratings_stats", reportDir)
    
    // ============================================================================
    // 3. INFORMACIÓN BÁSICA DEL DATASET JOINEADO (FULL)
    // ============================================================================
    println("\n" + "="*80)
    println("3. DATASET JOINEADO: MOVIES + RATINGS")
    println("="*80)
    
    println("\n--- Dimensiones del dataset joineado ---")
    val fullCount = fullDF.count()
    val fullColumns = fullDF.columns.length
    println(s"Total de filas: $fullCount")
    println(s"Total de columnas: $fullColumns")
    println(s"Diferencia con movies original: ${moviesCount - fullCount} filas (${((moviesCount - fullCount).toDouble / moviesCount * 100)}%.2f%% perdidas en el join)")
    
    println("\n--- Columnas disponibles en dataset joineado ---")
    fullDF.columns.zipWithIndex.foreach { case (col, idx) =>
      println(f"${idx + 1}%2d. $col")
    }
    
    println("\n--- Valores nulos por columna (dataset joineado) ---")
    val fullNullCounts = fullDF.columns.map { colName =>
      (colName, fullDF.filter(col(colName).isNull || col(colName) === "").count())
    }
    fullNullCounts.foreach { case (colName, nullCount) =>
      val percentage = (nullCount.toDouble / fullCount * 100)
      println(f"${colName}%-35s: ${nullCount}%6d nulos (${percentage}%5.2f%%)")
    }
    
    println("\n--- Muestra de datos joineados (primeras 5 filas) ---")
    println(s"Total de columnas en el dataset joineado: ${fullDF.columns.length}")
    mostrarTablaLegible(fullDF, 5, "joined_muestra", reportDir)
    
    println("\n--- Estadísticas combinadas ---")
    mostrarTablaLegible(fullDF.select("year", "duration", "avg_vote", "votes", "weighted_average_vote", "total_votes").describe(), 100, "joined_stats", reportDir)
    
    // ============================================================================
    // 4. ANÁLISIS ADICIONAL
    // ============================================================================
    println("\n" + "="*80)
    println("4. ANÁLISIS ADICIONAL")
    println("="*80)
    
    println("\n--- Registros únicos por columna clave ---")
    println(s"Películas únicas (imdb_title_id): ${fullDF.select("imdb_title_id").distinct().count()}")
    println(s"Años únicos: ${fullDF.select("year").distinct().count()}")
    println(s"Países únicos: ${fullDF.select("country").distinct().count()}")
    println(s"Idiomas únicos: ${fullDF.select("language").distinct().count()}")
    
    println("\n--- Top 10 años con más películas ---")
    mostrarTablaLegible(
      fullDF.groupBy("year")
        .count()
        .orderBy(col("count").desc),
      10,
      "top_years",
      reportDir
    )
    
    println("\n--- Distribución de calidad de datos ---")
    val registrosCompletos = fullDF.na.drop().count()
    val registrosConNulos = fullCount - registrosCompletos
    println(s"Registros sin ningún valor nulo: $registrosCompletos (${(registrosCompletos.toDouble / fullCount * 100)}%.2f%%)")
    println(s"Registros con al menos un valor nulo: $registrosConNulos (${(registrosConNulos.toDouble / fullCount * 100)}%.2f%%)")
    
    println("\n" + "="*80)
    println("ANÁLISIS EXPLORATORIO COMPLETADO")
    println("="*80)
    println(s"\n📁 Reportes guardados en: ${reportDir.getAbsolutePath()}/")
    println("💡 Para ver tablas anchas usa: less -S analisis_reportes/<archivo>.txt")
  }
  
  /**
   * Función auxiliar para mostrar tablas de forma más legible
   * - Muestra versión truncada en consola
   * - Guarda versión completa en archivo .txt para scroll horizontal
   * - Ofrece opción de formato vertical para mejor lectura
   */
  def mostrarTablaLegible(df: DataFrame, numRows: Int, nombre: String, reportDir: File): Unit = {
    // Versión para consola (truncada)
    println(s"\n[Consola - Truncado a 40 caracteres]")
    df.show(numRows, truncate = 40)
    
    // Versión completa guardada en archivo
    val archivoPath = s"${reportDir.getPath()}/${nombre}.txt"
    val writer = new PrintWriter(new File(archivoPath))
    try {
      // Capturar la salida de show() sin truncamiento
      import java.io.ByteArrayOutputStream
      import java.io.PrintStream
      val outputStream = new ByteArrayOutputStream()
      val printStream = new PrintStream(outputStream)
      Console.withOut(printStream) {
        df.show(numRows, truncate = false)
      }
      writer.write(outputStream.toString("UTF-8"))
    } finally {
      writer.close()
    }
    println(s"✓ Tabla completa guardada en: $archivoPath")
    
    // Si hay pocas filas, mostrar también en formato vertical
    if (numRows <= 5) {
      println(s"\n[Formato Vertical - Más legible]")
      mostrarFormatoVertical(df, numRows)
    }
  }
  
  /**
   * Muestra datos en formato vertical (clave: valor)
   * Muy útil para ver registros individuales con muchas columnas
   */
  def mostrarFormatoVertical(df: DataFrame, numRows: Int): Unit = {
    val rows = df.take(numRows)
    rows.zipWithIndex.foreach { case (row, idx) =>
      println(s"\n--- Registro ${idx + 1} ---")
      df.columns.zip(row.toSeq).foreach { case (colName, value) =>
        println(f"  $colName%-30s: $value")
      }
    }
  }
}

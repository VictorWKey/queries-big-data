import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

/**
 * Versión personalizable de las consultas IMDB
 * Puedes modificar los parámetros al inicio del main() para cambiar las consultas
 */
object IMDBCustom {
  
  def main(args: Array[String]): Unit = {
    // ========== PARÁMETROS PERSONALIZABLES ==========
    // Cambia estos valores para modificar las consultas
    
    val params = Map(
      // Consulta 1: Intervalo de años
      "anio_inicio_1" -> 2015,
      "anio_fin_1" -> 2020,
      
      // Consulta 2: Puntuación mínima
      "puntuacion_min" -> 8.5,
      
      // Consulta 3: Género
      "genero_1" -> "Drama",
      
      // Consulta 4: Dos géneros
      "genero_2a" -> "Horror",
      "genero_2b" -> "Drama",
      
      // Consulta 5: Nombre similar
      "nombre_busqueda" -> "The",
      
      // Consulta 6: Compuesta
      "anio_inicio_6" -> 2010,
      "anio_fin_6" -> 2020,
      "genero_6" -> "Action",
      "calificacion_6" -> 7.5,
      
      // Consulta 7: Actor
      "actor_7" -> "Leonardo DiCaprio",
      
      // Consulta 8: Actor + años
      "actor_8" -> "Tom Hanks",
      "anio_inicio_8" -> 2000,
      "anio_fin_8" -> 2020,
      
      // Consulta 9: Actor + años + género
      "actor_9" -> "Robert Downey Jr.",
      "anio_inicio_9" -> 2010,
      "anio_fin_9" -> 2020,
      "genero_9" -> "Action"
    )
    
    // ================================================
    
    // Inicializar Spark
    val spark = SparkSession.builder()
      .appName("IMDB Movies Custom")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    // Cargar datasets
    val moviesPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv"
    val ratingsPath = "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb ratings.csv"
    
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
    fullDF.createOrReplaceTempView("movies")
    
    println("\n" + "="*80)
    println("CONSULTAS PERSONALIZADAS IMDB - SPARK")
    println("="*80 + "\n")
    
    // Ejecutar consultas con parámetros personalizados
    ejecutarConsultas(spark, fullDF, params)
    
    spark.stop()
  }
  
  def ejecutarConsultas(spark: SparkSession, df: DataFrame, params: Map[String, Any]): Unit = {
    
    // Consulta 1
    println("\n" + "="*80)
    println(s"1. Películas entre ${params("anio_inicio_1")} y ${params("anio_fin_1")}")
    println("="*80)
    df.filter(col("year").between(params("anio_inicio_1").asInstanceOf[Int], 
                                   params("anio_fin_1").asInstanceOf[Int]))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("year").desc)
      .show(20, false)
    
    // Consulta 2
    println("\n" + "="*80)
    println(s"2. Películas con puntuación >= ${params("puntuacion_min")}")
    println("="*80)
    df.filter(col("avg_vote") >= params("puntuacion_min").asInstanceOf[Double])
      .select("title", "year", "avg_vote", "genre", "director", "votes")
      .orderBy(col("avg_vote").desc, col("votes").desc)
      .show(20, false)
    
    // Consulta 3
    println("\n" + "="*80)
    println(s"3. Películas de género '${params("genero_1")}'")
    println("="*80)
    df.filter(col("genre").contains(params("genero_1").asInstanceOf[String]))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
      .show(20, false)
    
    // Consulta 4
    println("\n" + "="*80)
    println(s"4. Películas de '${params("genero_2a")}' o '${params("genero_2b")}'")
    println("="*80)
    df.filter(col("genre").contains(params("genero_2a").asInstanceOf[String]) || 
              col("genre").contains(params("genero_2b").asInstanceOf[String]))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
      .show(20, false)
    
    // Consulta 5
    println("\n" + "="*80)
    println(s"5. Películas que empiezan con '${params("nombre_busqueda")}'")
    println("="*80)
    df.filter(col("title").startsWith(params("nombre_busqueda").asInstanceOf[String]))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
      .show(20, false)
    
    // Consulta 6
    println("\n" + "="*80)
    println(s"6. Películas ${params("anio_inicio_6")}-${params("anio_fin_6")}, " +
            s"'${params("genero_6")}', calificación >= ${params("calificacion_6")}")
    println("="*80)
    df.filter(
      col("year").between(params("anio_inicio_6").asInstanceOf[Int], 
                         params("anio_fin_6").asInstanceOf[Int]) &&
      col("genre").contains(params("genero_6").asInstanceOf[String]) &&
      col("avg_vote") >= params("calificacion_6").asInstanceOf[Double]
    )
    .select("title", "year", "genre", "avg_vote", "director", "votes")
    .orderBy(col("avg_vote").desc)
    .show(20, false)
    
    // Consulta 7
    println("\n" + "="*80)
    println(s"7. Películas de '${params("actor_7")}'")
    println("="*80)
    df.filter(col("actors").contains(params("actor_7").asInstanceOf[String]))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
      .show(20, false)
    
    // Consulta 8
    println("\n" + "="*80)
    println(s"8. Películas de '${params("actor_8")}' " +
            s"(${params("anio_inicio_8")}-${params("anio_fin_8")})")
    println("="*80)
    df.filter(
      col("actors").contains(params("actor_8").asInstanceOf[String]) &&
      col("year").between(params("anio_inicio_8").asInstanceOf[Int], 
                         params("anio_fin_8").asInstanceOf[Int])
    )
    .select("title", "year", "genre", "avg_vote", "director")
    .orderBy(col("avg_vote").desc)
    .show(20, false)
    
    // Consulta 9
    println("\n" + "="*80)
    println(s"9. Películas de '${params("actor_9")}' " +
            s"(${params("anio_inicio_9")}-${params("anio_fin_9")}), " +
            s"género '${params("genero_9")}'")
    println("="*80)
    df.filter(
      col("actors").contains(params("actor_9").asInstanceOf[String]) &&
      col("year").between(params("anio_inicio_9").asInstanceOf[Int], 
                         params("anio_fin_9").asInstanceOf[Int]) &&
      col("genre").contains(params("genero_9").asInstanceOf[String])
    )
    .select("title", "year", "genre", "avg_vote", "director")
    .orderBy(col("avg_vote").desc)
    .show(20, false)
  }
}

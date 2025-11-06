import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object IMDBQueries {
  
  def main(args: Array[String]): Unit = {
    // Crear SparkSession
    val spark = SparkSession.builder()
      .appName("IMDB Movies Analysis")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    // Cargar los datasets
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
    
    // Unir los datasets
    val fullDF = moviesDF.join(ratingsDF, Seq("imdb_title_id"), "left")
    
    // Crear vista temporal para consultas SQL
    fullDF.createOrReplaceTempView("movies")
    
    println("\n" + "="*80)
    println("SISTEMA DE CONSULTAS IMDB - SPARK")
    println("="*80 + "\n")
    
    // Ejecutar todas las consultas predefinidas
    ejecutarConsultasPredefinidas(spark, fullDF)
    
    spark.stop()
  }
  
  def ejecutarConsultasPredefinidas(spark: SparkSession, df: DataFrame): Unit = {
    println("\n### CONSULTAS PREDEFINIDAS ###\n")
    
    // 1. Películas entre un intervalo de años
    println("\n" + "="*80)
    println("1. Películas entre 2015 y 2020")
    println("="*80)
    peliculasPorIntervaloAnios(df, 2015, 2020).show(20, false)
    
    // 2. Películas por puntuación IMDB (ordenadas)
    println("\n" + "="*80)
    println("2. Películas con puntuación IMDB >= 8.5 (ordenadas)")
    println("="*80)
    peliculasPorPuntuacion(df, 8.5).show(20, false)
    
    // 3. Películas por género (ordenadas por puntuación IMDB)
    println("\n" + "="*80)
    println("3. Películas de género 'Drama' (ordenadas por puntuación)")
    println("="*80)
    peliculasPorGenero(df, "Drama").show(20, false)
    
    // 4. Películas de dos géneros específicos
    println("\n" + "="*80)
    println("4. Películas de géneros 'Horror' y 'Drama'")
    println("="*80)
    peliculasPorDosGeneros(df, "Horror", "Drama").show(20, false)
    
    // 5. Películas con nombre similar
    println("\n" + "="*80)
    println("5. Películas con nombres similares a 'The'")
    println("="*80)
    peliculasPorNombreSimilar(df, "The").show(20, false)
    
    // 6. Películas por intervalo de años, género y calificación
    println("\n" + "="*80)
    println("6. Películas 2010-2020, género 'Action', calificación >= 7.5")
    println("="*80)
    peliculasCompuesto(df, 2010, 2020, "Action", 7.5).show(20, false)
    
    // 7. Películas de un actor específico
    println("\n" + "="*80)
    println("7. Películas de 'Leonardo DiCaprio' (ordenadas por puntuación)")
    println("="*80)
    peliculasPorActor(df, "Leonardo DiCaprio").show(20, false)
    
    // 8. Películas de un actor en intervalo de años
    println("\n" + "="*80)
    println("8. Películas de 'Tom Hanks' entre 2000 y 2020")
    println("="*80)
    peliculasPorActorYAnios(df, "Tom Hanks", 2000, 2020).show(20, false)
    
    // 9. Películas de un actor, años, género
    println("\n" + "="*80)
    println("9. Películas de 'Robert Downey Jr.' 2010-2020, género 'Action'")
    println("="*80)
    peliculasPorActorAniosGenero(df, "Robert Downey Jr.", 2010, 2020, "Action").show(20, false)
  }
  
  // ========== FUNCIONES DE CONSULTA ==========
  
  /**
   * 1. Todas las películas entre un intervalo de años dado
   */
  def peliculasPorIntervaloAnios(df: DataFrame, anioInicio: Int, anioFin: Int): DataFrame = {
    df.filter(col("year").between(anioInicio, anioFin))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("year").desc)
  }
  
  /**
   * 2. Todas las películas por Puntuación IMDB solicitada (ordenadas)
   */
  def peliculasPorPuntuacion(df: DataFrame, puntuacionMinima: Double): DataFrame = {
    df.filter(col("avg_vote") >= puntuacionMinima)
      .select("title", "year", "avg_vote", "genre", "director", "votes")
      .orderBy(col("avg_vote").desc, col("votes").desc)
  }
  
  /**
   * 3. Todas las películas por Género (ordenadas por puntuación IMDB)
   */
  def peliculasPorGenero(df: DataFrame, genero: String): DataFrame = {
    df.filter(col("genre").contains(genero))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
  }
  
  /**
   * 4. Todas las películas en base a dos géneros
   */
  def peliculasPorDosGeneros(df: DataFrame, genero1: String, genero2: String): DataFrame = {
    df.filter(col("genre").contains(genero1) || col("genre").contains(genero2))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
  }
  
  /**
   * 5. Todas las películas cuyo nombre sea similar a (Nombre solicitado)
   */
  def peliculasPorNombreSimilar(df: DataFrame, nombreBusqueda: String): DataFrame = {
    df.filter(col("title").startsWith(nombreBusqueda))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
  }
  
  /**
   * 6. Películas entre intervalo de años, género y calificación mínima
   */
  def peliculasCompuesto(df: DataFrame, anioInicio: Int, anioFin: Int, 
                         genero: String, calificacionMinima: Double): DataFrame = {
    df.filter(
      col("year").between(anioInicio, anioFin) &&
      col("genre").contains(genero) &&
      col("avg_vote") >= calificacionMinima
    )
    .select("title", "year", "genre", "avg_vote", "director", "votes")
    .orderBy(col("avg_vote").desc)
  }
  
  /**
   * 7. Todas las películas de un determinado actor (ordenadas por puntuación)
   */
  def peliculasPorActor(df: DataFrame, actor: String): DataFrame = {
    df.filter(col("actors").contains(actor))
      .select("title", "year", "genre", "avg_vote", "director", "actors")
      .orderBy(col("avg_vote").desc)
  }
  
  /**
   * 8. Películas de un actor en un intervalo de años
   */
  def peliculasPorActorYAnios(df: DataFrame, actor: String, 
                              anioInicio: Int, anioFin: Int): DataFrame = {
    df.filter(
      col("actors").contains(actor) &&
      col("year").between(anioInicio, anioFin)
    )
    .select("title", "year", "genre", "avg_vote", "director", "actors")
    .orderBy(col("avg_vote").desc)
  }
  
  /**
   * 9. Películas de un actor, intervalo de años y género
   */
  def peliculasPorActorAniosGenero(df: DataFrame, actor: String, 
                                   anioInicio: Int, anioFin: Int, 
                                   genero: String): DataFrame = {
    df.filter(
      col("actors").contains(actor) &&
      col("year").between(anioInicio, anioFin) &&
      col("genre").contains(genero)
    )
    .select("title", "year", "genre", "avg_vote", "director", "actors")
    .orderBy(col("avg_vote").desc)
  }
}

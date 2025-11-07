import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object IMDBQueriesCompact {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IMDB Movies Analysis - Compact")
      .master("local[*]")
      .config("spark.driver.memory", "6g")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
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
    
    println("Sistema de Consultas IMDB")
    
    ejecutarConsultasCompactas(spark, fullDF)
    
    spark.stop()
  }
  
  def ejecutarConsultasCompactas(spark: SparkSession, df: DataFrame): Unit = {
    
    // CONSULTA 1: Películas recientes (2015-2020)
    mostrarResultado("1", "Películas recientes (2015-2020)", {
      df.filter(col("year").between(2015, 2020))
        .select("title", "year", "avg_vote")
        .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 2: Películas mejor calificadas (>= 8.5)
    mostrarResultado("2", "Películas mejor calificadas (puntuación >= 8.5)", {
      df.filter(col("avg_vote") >= 8.5)
        .select("title", "year", "avg_vote", "votes")
        .orderBy(col("avg_vote").desc, col("votes").desc)
    })
    
    // CONSULTA 3: Top películas de Drama
    mostrarResultado("3", "Top películas de Drama", {
      df.filter(col("genre").contains("Drama"))
        .select("title", "year", "avg_vote")
        .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 4: Películas de Horror o Thriller
    mostrarResultado("4", "Películas de Horror o Thriller", {
      df.filter(col("genre").contains("Horror") || col("genre").contains("Thriller"))
        .select("title", "year", "avg_vote")
        .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 5: Películas "The Dark..."
    mostrarResultado("5", "Películas que comienzan con 'The Dark'", {
      df.filter(col("title").startsWith("The Dark"))
        .select("title", "year", "avg_vote")
        .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 6: Action 2010-2020 bien calificadas
    mostrarResultado("6", "Películas Action 2010-2020 (calificación >= 7.5)", {
      df.filter(
        col("year").between(2010, 2020) &&
        col("genre").contains("Action") &&
        col("avg_vote") >= 7.5
      )
      .select("title", "year", "avg_vote")
      .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 7: Leonardo DiCaprio
    mostrarResultado("7", "Mejores películas de Leonardo DiCaprio", {
      df.filter(col("actors").contains("Leonardo DiCaprio"))
        .select("title", "year", "avg_vote")
        .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 8: Tom Hanks 2000-2020
    mostrarResultado("8", "Películas de Tom Hanks (2000-2020)", {
      df.filter(
        col("actors").contains("Tom Hanks") &&
        col("year").between(2000, 2020)
      )
      .select("title", "year", "avg_vote")
      .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 9: Robert Downey Jr. Action
    mostrarResultado("9", "Robert Downey Jr. en películas de Action (2010-2020)", {
      df.filter(
        col("actors").contains("Robert Downey Jr.") &&
        col("year").between(2010, 2020) &&
        col("genre").contains("Action")
      )
      .select("title", "year", "avg_vote")
      .orderBy(col("avg_vote").desc)
    })
    
    println("Todas las consultas completadas")
  }
  
  def mostrarResultado(numero: String, titulo: String, resultado: => DataFrame): Unit = {
    println(s"\nConsulta $numero: $titulo")
    
    val df = resultado
    val total = df.count()
    
    if (total > 0) {
      println(s"Total encontradas: $total películas")
      println("Top 10 resultados:")
      df.show(10, truncate = 40)
    } else {
      println("No se encontraron resultados para esta consulta.")
    }
  }
}

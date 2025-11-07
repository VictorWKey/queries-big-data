import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// :load IMDBQueriesCompact.scala
// IMDBQueriesCompact.main(Array())

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
    
    println("Sistema de Consultas - Películas")
    
    ejecutarConsultasCompactas(spark, fullDF)
    
    spark.stop()
  }
  
  def ejecutarConsultasCompactas(spark: SparkSession, df: DataFrame): Unit = {
    
    // CONSULTA 1: Todas las películas entre un intervalo de años dado.
    mostrarResultado("1", "Películas recientes (2015-2020)", {
      df.filter(col("year").between(2015, 2020))
        .select("title", "year", "avg_vote")
        .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 2: Todas las películas por Puntuación IMDB solicitada (ordenadas).
    mostrarResultado("2", "Todas las películas por Puntuación IMDB solicitada (ordenadas)", {
      df.filter(col("avg_vote") >= 8.5)
        .select("title", "year", "avg_vote", "votes")
        .orderBy(col("avg_vote").desc, col("votes").desc)
    })
    
    // CONSULTA 3: Todas las películas por Clasificación (ordenadas por puntuación IMDB).
    mostrarResultado("3", "Todas las películas por Clasificación (ordenadas por puntuación IMDB)", {
      df.filter(col("genre").contains("Drama"))
        .select("title", "year", "avg_vote")
        .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 4: Todas las películas en base a dos clasificaciones (Terror, Drama).
    mostrarResultado("4", "Todas las películas en base a dos clasificaciones (Terror, Drama)", {
      df.filter(col("genre").contains("Horror") || col("genre").contains("Thriller"))
        .select("title", "year", "avg_vote")
        .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 5: Todas las películas cuyo nombre sea similar a (Nombre solicitado), por ejemplo: "Los..." (Los Increíbles, Los Expedientes X, Los 7 Magníficos...)
    mostrarResultado("5", "Todas las películas cuyo nombre sea similar a (Nombre solicitado)", {
      df.filter(col("title").startsWith("The Dark"))
        .select("title", "year", "avg_vote")
        .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 6: Todas las películas entre un intervalo de años, en base a un género y superiores o iguales a cierta calificación IMDB.
    mostrarResultado("6", "Todas las películas Entre un intervalo de años, en base a un género y superiores o iguales a cierta calificación IMDB", {
      df.filter(
        col("year").between(2010, 2020) &&
        col("genre").contains("Action") &&
        col("avg_vote") >= 7.5
      )
      .select("title", "year", "avg_vote")
      .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 7: Todas las películas de un determinado actor (ordenadas por puntuación IMDB).
    mostrarResultado("7", "Todas las películas de un determinado actor (ordenadas por puntuación IMDB)", {
      df.filter(col("actors").contains("Leonardo DiCaprio"))
        .select("title", "year", "avg_vote")
        .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 8: Todas las películas de un determinado actor en un intervalo de años (ordenadas por puntuación IMDB).
    mostrarResultado("8", "Todas las películas de un determinado actor en un intervalo de años (ordenadas por puntuación IMDB)", {
      df.filter(
        col("actors").contains("Tom Hanks") &&
        col("year").between(2000, 2020)
      )
      .select("title", "year", "avg_vote")
      .orderBy(col("avg_vote").desc)
    })
    
    // CONSULTA 9: Todas las películas de un determinado actor en un intervalo de años, de determinada clasificación y determinado género.
    mostrarResultado("9", "Todas las películas de un determinado actor en un intervalo de años, de determinada clasificación y determinado género.", {
      df.filter(
        col("actors").contains("Robert Downey Jr.") &&
        col("year").between(2010, 2020) &&
        col("genre").contains("Action")
      )
      .select("title", "year", "avg_vote")
      .orderBy(col("avg_vote").desc)
    })
    
    println("Consultas completadas")
  }
  
  def mostrarResultado(numero: String, titulo: String, resultado: => DataFrame): Unit = {
    println(s"Consulta $numero: $titulo")
    
    val df = resultado
    val total = df.count()
    
    if (total > 0) {
      println(s"Total encontradas: $total registros")
      println("Top 10 resultados (muestra):")
      df.show(10, truncate = 40)
    } else {
      println("No se encontraron resultados para esta consulta.")
    }
  }
}

file://<WORKSPACE>/IMDBQueriesSQL.scala
empty definition using pc, found symbol in pc: 
semanticdb not found
empty definition using fallback
non-local guesses:
	 -query.
	 -query#
	 -query().
	 -scala/Predef.query.
	 -scala/Predef.query#
	 -scala/Predef.query().
offset: 8060
uri: file://<WORKSPACE>/IMDBQueriesSQL.scala
text:
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

// :load IMDBQueriesSQL.scala
// IMDBQueriesSQL.main(Array())

object IMDBQueriesSQL {
   
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IMDB Movies Analysis - SQL")
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
    
    // Registrar DataFrames como vistas temporales para poder usar SQL
    moviesDF.createOrReplaceTempView("movies")
    ratingsDF.createOrReplaceTempView("ratings")
    
    // Crear una vista con el JOIN
    val joinQuery = """
      SELECT m.*, r.weighted_average_vote, r.total_votes, r.mean_vote, 
             r.median_vote, r.votes_10, r.votes_9, r.votes_8, r.votes_7, 
             r.votes_6, r.votes_5, r.votes_4, r.votes_3, r.votes_2, r.votes_1,
             r.allgenders_0age_avg_vote, r.allgenders_0age_votes,
             r.allgenders_18age_avg_vote, r.allgenders_18age_votes,
             r.allgenders_30age_avg_vote, r.allgenders_30age_votes,
             r.allgenders_45age_avg_vote, r.allgenders_45age_votes,
             r.males_allages_avg_vote, r.males_allages_votes,
             r.males_0age_avg_vote, r.males_0age_votes,
             r.males_18age_avg_vote, r.males_18age_votes,
             r.males_30age_avg_vote, r.males_30age_votes,
             r.males_45age_avg_vote, r.males_45age_votes,
             r.females_allages_avg_vote, r.females_allages_votes,
             r.females_0age_avg_vote, r.females_0age_votes,
             r.females_18age_avg_vote, r.females_18age_votes,
             r.females_30age_avg_vote, r.females_30age_votes,
             r.females_45age_avg_vote, r.females_45age_votes,
             r.top1000_voters_rating, r.top1000_voters_votes,
             r.us_voters_rating, r.us_voters_votes,
             r.non_us_voters_rating, r.non_us_voters_votes
      FROM movies m
      LEFT JOIN ratings r ON m.imdb_title_id = r.imdb_title_id
    """
    
    val fullDF = spark.sql(joinQuery)
    fullDF.createOrReplaceTempView("full_movies")
    
    println("Sistema de Consultas - Películas (SQL)")
    
    ejecutarConsultasSQL(spark)
    
    spark.stop()
  }
  
  def ejecutarConsultasSQL(spark: SparkSession): Unit = {
    
    // // CONSULTA 1: Todas las películas entre un intervalo de años dado.
    // mostrarResultado("1", "Películas recientes (2015-2020)", {
    //   val query = """
    //     SELECT title, year, avg_vote
    //     FROM full_movies
    //     WHERE year BETWEEN 2015 AND 2020
    //     ORDER BY avg_vote DESC
    //   """
    //   spark.sql(query)
    // })
    
    // // CONSULTA 2: Todas las películas por Puntuación IMDB solicitada (ordenadas).
    // mostrarResultado("2", "Todas las películas por Puntuación IMDB solicitada (ordenadas)", {
    //   val query = """
    //     SELECT title, year, avg_vote, votes
    //     FROM full_movies
    //     WHERE avg_vote >= 8.5
    //     ORDER BY avg_vote DESC, votes DESC
    //   """
    //   spark.sql(query)
    // })
    
    // // CONSULTA 3: Todas las películas por Clasificación (ordenadas por puntuación IMDB).
    // mostrarResultado("3", "Todas las películas por Clasificación (ordenadas por puntuación IMDB)", {
    //   val query = """
    //     SELECT title, year, avg_vote
    //     FROM full_movies
    //     WHERE genre LIKE '%Drama%'
    //     ORDER BY avg_vote DESC
    //   """
    //   spark.sql(query)
    // })
    
    // // CONSULTA 4: Todas las películas en base a dos clasificaciones (Terror, Drama).
    // mostrarResultado("4", "Todas las películas en base a dos clasificaciones (Terror, Drama)", {
    //   val query = """
    //     SELECT title, year, avg_vote
    //     FROM full_movies
    //     WHERE genre LIKE '%Horror%' OR genre LIKE '%Thriller%'
    //     ORDER BY avg_vote DESC
    //   """
    //   spark.sql(query)
    // })
    
    // // CONSULTA 5: Todas las películas cuyo nombre sea similar a (Nombre solicitado), por ejemplo: "Los..." (Los Increíbles, Los Expedientes X, Los 7 Magníficos...)
    // mostrarResultado("5", "Todas las películas cuyo nombre sea similar a (Nombre solicitado)", {
    //   val query = """
    //     SELECT title, year, avg_vote
    //     FROM full_movies
    //     WHERE title LIKE 'The Dark%'
    //     ORDER BY avg_vote DESC
    //   """
    //   spark.sql(query)
    // })
    
    // // CONSULTA 6: Todas las películas entre un intervalo de años, en base a un género y superiores o iguales a cierta calificación IMDB.
    // mostrarResultado("6", "Todas las películas Entre un intervalo de años, en base a un género y superiores o iguales a cierta calificación IMDB", {
    //   val query = """
    //     SELECT title, year, avg_vote
    //     FROM full_movies
    //     WHERE year BETWEEN 2010 AND 2020
    //       AND genre LIKE '%Action%'
    //       AND avg_vote >= 7.5
    //     ORDER BY avg_vote DESC
    //   """
    //   spark.sql(query)
    // })
    
    // // CONSULTA 7: Todas las películas de un determinado actor (ordenadas por puntuación IMDB).
    // mostrarResultado("7", "Todas las películas de un determinado actor (ordenadas por puntuación IMDB)", {
    //   val query = """
    //     SELECT title, year, avg_vote
    //     FROM full_movies
    //     WHERE actors LIKE '%Leonardo DiCaprio%'
    //     ORDER BY avg_vote DESC
    //   """
    //   spark.sql(query)
    // })
    
    // // CONSULTA 8: Todas las películas de un determinado actor en un intervalo de años (ordenadas por puntuación IMDB).
    // mostrarResultado("8", "Todas las películas de un determinado actor en un intervalo de años (ordenadas por puntuación IMDB)", {
    //   val query = """
    //     SELECT title, year, avg_vote
    //     FROM full_movies
    //     WHERE actors LIKE '%Tom Hanks%'
    //       AND year BETWEEN 2000 AND 2020
    //     ORDER BY avg_vote DESC
    //   """
    //   spark.sql(query)
    // })
    
    // // CONSULTA 9: Todas las películas de un determinado actor en un intervalo de años, de determinada clasificación y determinado género.
    // mostrarResultado("9", "Todas las películas de un determinado actor en un intervalo de años, de determinada clasificación y determinado género.", {
    //   val query = """
    //     SELECT title, year, avg_vote
    //     FROM full_movies
    //     WHERE actors LIKE '%Robert Downey Jr.%'
    //       AND year BETWEEN 2010 AND 2020
    //       AND genre LIKE '%Action%'
    //     ORDER BY avg_vote DESC
    //   """
    //   spark.sql(query)
    // })

    mostrarResultado("1", "avg_vote total de todas las peliculas", {
      val query = """
        SELECT AVG(avg_vote) AS avg_vote_total
        FROM full_movies
      """
      spark.sql(query)
    })

    mostrarResultado("1", "media de la columna total_votes", {
      val query = """
        SELECT AVG(total_votes) AS avg_total_votes
        FROM full_movies
      """
      spark.sql(query)
    })


    mostrarResultado("1", "desviacion estandar de total_votes", {
      val query = """
        SELECT STDDEV(total_votes) AS stddev_total_votes
        FROM full_movies
      """
      spark.sql(query)
    })

    mostrarResultado("1", "outlier mediante 3 veces la desviacion estandar ordenados ordenadas", {
      val query = """
        SELECT original_title, total_votes
        FROM full_movies
        WHERE total_votes > (
          SELECT AVG(total_votes) + 3 * STDDEV(total_votes)
          FROM full_movies
        )
        ORDER BY total_votes DESC
      """
      spark.sql(que@@ry)
    })
    
    println("Consultas SQL completadas")
  }
  
  def mostrarResultado(numero: String, titulo: String, resultado: => DataFrame): Unit = {
    println(s"Consulta $numero: $titulo")
    
    val df = resultado
    val total = df.count()
    
    if (total > 0) {
      println(s"Total encontradas: $total registros")
      println("Top 10 resultados (muestra):")
      df.show(20, truncate = 40)
    } else {
      println("No se encontraron resultados para esta consulta.")
    }
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 
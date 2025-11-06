import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import scala.io.StdIn

object IMDBInteractive {
  
  var spark: SparkSession = _
  var fullDF: DataFrame = _
  
  def main(args: Array[String]): Unit = {
    // Inicializar Spark
    spark = SparkSession.builder()
      .appName("IMDB Movies - Interactive")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    // Cargar datasets
    cargarDatos()
    
    // Menú interactivo
    menuPrincipal()
    
    spark.stop()
  }
  
  def cargarDatos(): Unit = {
    println("\n" + "="*80)
    println("Cargando datos de IMDB...")
    println("="*80 + "\n")
    
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
    
    fullDF = moviesDF.join(ratingsDF, Seq("imdb_title_id"), "left")
    fullDF.createOrReplaceTempView("movies")
    
    println(s"✓ Total de películas cargadas: ${fullDF.count()}")
    println("✓ Datos listos para consultas\n")
  }
  
  def menuPrincipal(): Unit = {
    var continuar = true
    
    while (continuar) {
      println("\n" + "="*80)
      println("SISTEMA DE CONSULTAS IMDB - MODO INTERACTIVO")
      println("="*80)
      println("\nSelecciona una consulta:")
      println("1.  Películas por intervalo de años")
      println("2.  Películas por puntuación IMDB mínima")
      println("3.  Películas por género")
      println("4.  Películas de dos géneros")
      println("5.  Películas con nombre similar")
      println("6.  Películas por años + género + calificación")
      println("7.  Películas por actor")
      println("8.  Películas por actor + años")
      println("9.  Películas por actor + años + género")
      println("10. Consulta SQL personalizada")
      println("11. Estadísticas generales")
      println("0.  Salir")
      println("\nOpción: ")
      
      val opcion = scala.io.StdIn.readLine()
      
      opcion match {
        case "1" => consulta1()
        case "2" => consulta2()
        case "3" => consulta3()
        case "4" => consulta4()
        case "5" => consulta5()
        case "6" => consulta6()
        case "7" => consulta7()
        case "8" => consulta8()
        case "9" => consulta9()
        case "10" => consultaSQL()
        case "11" => estadisticas()
        case "0" => continuar = false
        case _ => println("Opción no válida")
      }
    }
    
    println("\n¡Hasta luego!")
  }
  
  def consulta1(): Unit = {
    println("\n--- Películas por intervalo de años ---")
    print("Año inicial: ")
    val inicio = scala.io.StdIn.readInt()
    print("Año final: ")
    val fin = scala.io.StdIn.readInt()
    print("¿Cuántos resultados mostrar? (default 20): ")
    val limite = try { scala.io.StdIn.readInt() } catch { case _: Exception => 20 }
    
    val resultado = fullDF
      .filter(col("year").between(inicio, fin))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("year").desc)
    
    println(s"\nTotal encontradas: ${resultado.count()}")
    resultado.show(limite, false)
  }
  
  def consulta2(): Unit = {
    println("\n--- Películas por puntuación IMDB ---")
    print("Puntuación mínima (0-10): ")
    val puntuacion = scala.io.StdIn.readDouble()
    print("¿Cuántos resultados mostrar? (default 20): ")
    val limite = try { scala.io.StdIn.readInt() } catch { case _: Exception => 20 }
    
    val resultado = fullDF
      .filter(col("avg_vote") >= puntuacion)
      .select("title", "year", "avg_vote", "genre", "director", "votes")
      .orderBy(col("avg_vote").desc, col("votes").desc)
    
    println(s"\nTotal encontradas: ${resultado.count()}")
    resultado.show(limite, false)
  }
  
  def consulta3(): Unit = {
    println("\n--- Películas por género ---")
    println("Géneros disponibles: Action, Comedy, Drama, Horror, Romance, Sci-Fi, Thriller, etc.")
    print("Género: ")
    val genero = scala.io.StdIn.readLine()
    print("¿Cuántos resultados mostrar? (default 20): ")
    val limite = try { scala.io.StdIn.readInt() } catch { case _: Exception => 20 }
    
    val resultado = fullDF
      .filter(col("genre").contains(genero))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
    
    println(s"\nTotal encontradas: ${resultado.count()}")
    resultado.show(limite, false)
  }
  
  def consulta4(): Unit = {
    println("\n--- Películas de dos géneros ---")
    print("Primer género: ")
    val genero1 = scala.io.StdIn.readLine()
    print("Segundo género: ")
    val genero2 = scala.io.StdIn.readLine()
    print("¿Cuántos resultados mostrar? (default 20): ")
    val limite = try { scala.io.StdIn.readInt() } catch { case _: Exception => 20 }
    
    val resultado = fullDF
      .filter(col("genre").contains(genero1) || col("genre").contains(genero2))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
    
    println(s"\nTotal encontradas: ${resultado.count()}")
    resultado.show(limite, false)
  }
  
  def consulta5(): Unit = {
    println("\n--- Películas con nombre similar ---")
    print("Texto de búsqueda (inicio del título): ")
    val busqueda = scala.io.StdIn.readLine()
    print("¿Cuántos resultados mostrar? (default 20): ")
    val limite = try { scala.io.StdIn.readInt() } catch { case _: Exception => 20 }
    
    val resultado = fullDF
      .filter(col("title").startsWith(busqueda))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
    
    println(s"\nTotal encontradas: ${resultado.count()}")
    resultado.show(limite, false)
  }
  
  def consulta6(): Unit = {
    println("\n--- Películas por años + género + calificación ---")
    print("Año inicial: ")
    val inicio = scala.io.StdIn.readInt()
    print("Año final: ")
    val fin = scala.io.StdIn.readInt()
    print("Género: ")
    val genero = scala.io.StdIn.readLine()
    print("Calificación mínima: ")
    val calificacion = scala.io.StdIn.readDouble()
    print("¿Cuántos resultados mostrar? (default 20): ")
    val limite = try { scala.io.StdIn.readInt() } catch { case _: Exception => 20 }
    
    val resultado = fullDF
      .filter(
        col("year").between(inicio, fin) &&
        col("genre").contains(genero) &&
        col("avg_vote") >= calificacion
      )
      .select("title", "year", "genre", "avg_vote", "director", "votes")
      .orderBy(col("avg_vote").desc)
    
    println(s"\nTotal encontradas: ${resultado.count()}")
    resultado.show(limite, false)
  }
  
  def consulta7(): Unit = {
    println("\n--- Películas por actor ---")
    print("Nombre del actor: ")
    val actor = scala.io.StdIn.readLine()
    print("¿Cuántos resultados mostrar? (default 20): ")
    val limite = try { scala.io.StdIn.readInt() } catch { case _: Exception => 20 }
    
    val resultado = fullDF
      .filter(col("actors").contains(actor))
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
    
    println(s"\nTotal encontradas: ${resultado.count()}")
    resultado.show(limite, false)
  }
  
  def consulta8(): Unit = {
    println("\n--- Películas por actor + años ---")
    print("Nombre del actor: ")
    val actor = scala.io.StdIn.readLine()
    print("Año inicial: ")
    val inicio = scala.io.StdIn.readInt()
    print("Año final: ")
    val fin = scala.io.StdIn.readInt()
    print("¿Cuántos resultados mostrar? (default 20): ")
    val limite = try { scala.io.StdIn.readInt() } catch { case _: Exception => 20 }
    
    val resultado = fullDF
      .filter(
        col("actors").contains(actor) &&
        col("year").between(inicio, fin)
      )
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
    
    println(s"\nTotal encontradas: ${resultado.count()}")
    resultado.show(limite, false)
  }
  
  def consulta9(): Unit = {
    println("\n--- Películas por actor + años + género ---")
    print("Nombre del actor: ")
    val actor = scala.io.StdIn.readLine()
    print("Año inicial: ")
    val inicio = scala.io.StdIn.readInt()
    print("Año final: ")
    val fin = scala.io.StdIn.readInt()
    print("Género: ")
    val genero = scala.io.StdIn.readLine()
    print("¿Cuántos resultados mostrar? (default 20): ")
    val limite = try { scala.io.StdIn.readInt() } catch { case _: Exception => 20 }
    
    val resultado = fullDF
      .filter(
        col("actors").contains(actor) &&
        col("year").between(inicio, fin) &&
        col("genre").contains(genero)
      )
      .select("title", "year", "genre", "avg_vote", "director")
      .orderBy(col("avg_vote").desc)
    
    println(s"\nTotal encontradas: ${resultado.count()}")
    resultado.show(limite, false)
  }
  
  def consultaSQL(): Unit = {
    println("\n--- Consulta SQL Personalizada ---")
    println("Tabla disponible: movies")
    println("Columnas principales: title, year, genre, avg_vote, director, actors, country, language, votes")
    println("\nEjemplo: SELECT title, year, avg_vote FROM movies WHERE year > 2020 ORDER BY avg_vote DESC LIMIT 10")
    print("\nIngresa tu consulta SQL: ")
    val query = scala.io.StdIn.readLine()
    
    try {
      val resultado = spark.sql(query)
      println(s"\nTotal de filas: ${resultado.count()}")
      resultado.show(50, false)
    } catch {
      case e: Exception => println(s"Error en la consulta: ${e.getMessage}")
    }
  }
  
  def estadisticas(): Unit = {
    println("\n" + "="*80)
    println("ESTADÍSTICAS GENERALES DEL DATASET")
    println("="*80)
    
    println("\n--- Total de películas ---")
    println(s"${fullDF.count()} películas en total")
    
    println("\n--- Top 10 géneros más comunes ---")
    fullDF
      .filter(col("genre").isNotNull)
      .select(explode(split(col("genre"), ", ")).alias("genre_individual"))
      .groupBy("genre_individual")
      .count()
      .orderBy(col("count").desc)
      .show(10, false)
    
    println("\n--- Películas por década ---")
    fullDF
      .filter(col("year").isNotNull)
      .withColumn("decade", (floor(col("year") / 10) * 10).cast("int"))
      .groupBy("decade")
      .count()
      .orderBy("decade")
      .show(false)
    
    println("\n--- Top 10 directores con más películas ---")
    fullDF
      .filter(col("director").isNotNull)
      .groupBy("director")
      .count()
      .orderBy(col("count").desc)
      .show(10, false)
    
    println("\n--- Top 10 películas mejor calificadas (con más de 10000 votos) ---")
    fullDF
      .filter(col("votes") > 10000)
      .select("title", "year", "avg_vote", "votes", "director")
      .orderBy(col("avg_vote").desc)
      .show(10, false)
    
    println("\n--- Estadísticas de calificaciones ---")
    fullDF
      .select(
        avg("avg_vote").alias("Promedio"),
        min("avg_vote").alias("Mínima"),
        max("avg_vote").alias("Máxima"),
        stddev("avg_vote").alias("Desv. Estándar")
      )
      .show(false)
  }
}

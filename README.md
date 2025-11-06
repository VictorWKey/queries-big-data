# Sistema de Consultas IMDB con Apache Spark

## 📋 Descripción

Este proyecto implementa un sistema completo de consultas sobre el dataset de IMDB usando **Scala** y **Apache Spark**. Incluye 9 consultas predefinidas y un modo interactivo para consultas en vivo.

## 🚀 Instalación

### 1. Verificar que Spark esté instalado

```bash
spark-shell --version
```

Si no está instalado, ejecuta:

```bash
./install-spark.sh
source ~/.zshrc
```

### 2. Verificar estructura del proyecto

```
queries-big-data/
├── IMDB-Movies-Extensive-Dataset-Analysis/
│   └── data1/
│       ├── IMDb movies.csv
│       └── IMDb ratings.csv
├── IMDBQueries.scala          # Consultas predefinidas
├── IMDBInteractive.scala      # Modo interactivo
├── run-predefined.sh          # Ejecutar consultas predefinidas
└── run-interactive.sh         # Ejecutar modo interactivo
```

## 🎯 Uso

### Opción 1: Ejecutar Consultas Predefinidas

Este modo ejecuta automáticamente las 9 consultas requeridas:

```bash
./run-predefined.sh
```

Las consultas incluyen:
1. ✅ Películas entre 2015 y 2020
2. ✅ Películas con puntuación >= 8.5
3. ✅ Películas de género Drama
4. ✅ Películas de Horror y Drama
5. ✅ Películas que empiezan con "The"
6. ✅ Películas 2010-2020, Action, calificación >= 7.5
7. ✅ Películas de Leonardo DiCaprio
8. ✅ Películas de Tom Hanks (2000-2020)
9. ✅ Películas de Robert Downey Jr. (2010-2020, Action)

### Opción 2: Modo Interactivo (Para Presentación)

Este modo te permite hacer consultas personalizadas en vivo:

```bash
./run-interactive.sh
```

**Menú disponible:**
- Consultas 1-9: Versiones interactivas de las consultas predefinidas
- Consulta SQL personalizada: Para cualquier consulta que te pida tu profesor
- Estadísticas generales: Análisis del dataset

### Opción 3: Spark Shell Manual

Si prefieres trabajar directamente en spark-shell:

```bash
cd /home/victorwkey/desktop/queries-big-data
spark-shell
```

Luego puedes cargar el archivo:

```scala
:load IMDBQueries.scala
```

## 📊 Consultas Implementadas

### 1. Películas por Intervalo de Años
```scala
peliculasPorIntervaloAnios(df, 2015, 2020)
```

### 2. Películas por Puntuación IMDB
```scala
peliculasPorPuntuacion(df, 8.5)
```

### 3. Películas por Género
```scala
peliculasPorGenero(df, "Drama")
```

### 4. Películas de Dos Géneros
```scala
peliculasPorDosGeneros(df, "Horror", "Drama")
```

### 5. Películas con Nombre Similar
```scala
peliculasPorNombreSimilar(df, "The")
```

### 6. Películas Compuestas (Años + Género + Calificación)
```scala
peliculasCompuesto(df, 2010, 2020, "Action", 7.5)
```

### 7. Películas por Actor
```scala
peliculasPorActor(df, "Leonardo DiCaprio")
```

### 8. Películas por Actor y Años
```scala
peliculasPorActorYAnios(df, "Tom Hanks", 2000, 2020)
```

### 9. Películas por Actor, Años y Género
```scala
peliculasPorActorAniosGenero(df, "Robert Downey Jr.", 2010, 2020, "Action")
```

## 🎓 Para la Presentación

### Tips para consultas en vivo:

1. **Usa el modo interactivo** (`./run-interactive.sh`)
2. **Opción 10 del menú** te permite hacer consultas SQL personalizadas
3. **Opción 11** muestra estadísticas impresionantes del dataset

### Ejemplos de consultas SQL personalizadas:

```sql
-- Top 10 películas más votadas
SELECT title, year, avg_vote, votes 
FROM movies 
WHERE votes > 100000 
ORDER BY votes DESC 
LIMIT 10

-- Películas por país
SELECT country, COUNT(*) as total 
FROM movies 
WHERE country IS NOT NULL 
GROUP BY country 
ORDER BY total DESC 
LIMIT 10

-- Mejores películas de la década de 2010
SELECT title, year, avg_vote, genre, director 
FROM movies 
WHERE year BETWEEN 2010 AND 2019 AND votes > 50000 
ORDER BY avg_vote DESC 
LIMIT 20

-- Actores más frecuentes
SELECT actors, COUNT(*) as peliculas 
FROM movies 
WHERE actors IS NOT NULL 
GROUP BY actors 
ORDER BY peliculas DESC 
LIMIT 10
```

## 🛠️ Tecnologías Utilizadas

- **Apache Spark 3.3.1**: Motor de procesamiento distribuido
- **Scala 2.12.15**: Lenguaje de programación
- **Spark SQL**: Para consultas estructuradas
- **DataFrames API**: Manipulación de datos

## 📁 Estructura del Dataset

**IMDb movies.csv** (85,855 películas):
- `imdb_title_id`: ID único
- `title`: Título de la película
- `year`: Año de lanzamiento
- `genre`: Géneros (separados por coma)
- `director`: Director
- `actors`: Actores principales
- `avg_vote`: Calificación promedio (0-10)
- `votes`: Número de votos
- `country`, `language`, `description`, etc.

**IMDb ratings.csv**:
- Información detallada de votaciones por demografía
- Datos complementarios de calificaciones

## 🔧 Troubleshooting

### Error: "spark-shell: command not found"
```bash
source ~/.zshrc
```

### Error al cargar los CSV
Verifica que estés en el directorio correcto:
```bash
cd /home/victorwkey/desktop/queries-big-data
```

### Memoria insuficiente
El script ya está configurado con 4GB de memoria. Si necesitas más:
```bash
# En los archivos .scala, modifica:
.config("spark.driver.memory", "8g")
```

## 📝 Notas

- Todas las consultas usan **únicamente Scala y Spark** como se requiere
- Los resultados se muestran en consola formateados
- El código está optimizado para datasets grandes
- Las funciones son reutilizables y modulares

## 👨‍💻 Autor

Proyecto de análisis de datos IMDB para la clase de Big Data

---

**¡Buena suerte en tu presentación! 🎬🎥**

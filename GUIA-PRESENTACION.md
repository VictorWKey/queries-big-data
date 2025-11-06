# 🎬 Guía Rápida para la Presentación

## ⚡ Inicio Rápido

```bash
cd /home/victorwkey/desktop/queries-big-data
./check-setup.sh  # Verificar que todo esté listo
```

## 📋 Demostración de Consultas Predefinidas

Para mostrar las 9 consultas requeridas:

```bash
./run-predefined.sh
```

**Duración estimada**: 2-3 minutos (carga de datos + ejecución de consultas)

### Lo que verá tu profesor:
1. ✅ Películas entre 2015 y 2020
2. ✅ Películas con puntuación >= 8.5 (ordenadas)
3. ✅ Películas de género Drama (ordenadas por puntuación)
4. ✅ Películas de Horror y Drama
5. ✅ Películas que empiezan con "The"
6. ✅ Películas 2010-2020, Action, calificación >= 7.5
7. ✅ Películas de Leonardo DiCaprio (ordenadas)
8. ✅ Películas de Tom Hanks entre 2000-2020
9. ✅ Películas de Robert Downey Jr. 2010-2020, Action

## 🎯 Modo Interactivo (Para Consultas en Vivo)

```bash
./run-interactive.sh
```

### Consultas Sugeridas para Impresionar

#### 1. Top películas de un director específico
- Opción: **7** (Películas por actor) - pero úsala con directores famosos
- Ejemplos: "Christopher Nolan", "Quentin Tarantino", "Steven Spielberg"

#### 2. Películas de acción de la última década
- Opción: **6** (Años + género + calificación)
- Año inicial: 2010
- Año final: 2020
- Género: Action
- Calificación: 7.0

#### 3. Consulta SQL personalizada (Opción 10)

**Ejemplo 1: Top 10 países productores de cine**
```sql
SELECT country, COUNT(*) as total_peliculas 
FROM movies 
WHERE country IS NOT NULL 
GROUP BY country 
ORDER BY total_peliculas DESC 
LIMIT 10
```

**Ejemplo 2: Mejores películas de Sci-Fi**
```sql
SELECT title, year, avg_vote, director 
FROM movies 
WHERE genre LIKE '%Sci-Fi%' AND votes > 50000 
ORDER BY avg_vote DESC 
LIMIT 15
```

**Ejemplo 3: Evolución del cine por década**
```sql
SELECT FLOOR(year/10)*10 as decada, 
       COUNT(*) as total_peliculas,
       ROUND(AVG(avg_vote), 2) as calificacion_promedio
FROM movies 
WHERE year IS NOT NULL 
GROUP BY decada 
ORDER BY decada
```

**Ejemplo 4: Actores más prolíficos**
```sql
SELECT actors, COUNT(*) as num_peliculas, 
       ROUND(AVG(avg_vote), 2) as promedio_calificacion
FROM movies 
WHERE actors IS NOT NULL 
GROUP BY actors 
ORDER BY num_peliculas DESC 
LIMIT 20
```

**Ejemplo 5: Películas con más votos (más populares)**
```sql
SELECT title, year, avg_vote, votes, genre 
FROM movies 
ORDER BY votes DESC 
LIMIT 20
```

#### 4. Estadísticas Generales (Opción 11)
Muestra análisis completo del dataset:
- Total de películas
- Géneros más comunes
- Películas por década
- Directores con más películas
- Top películas mejor calificadas
- Estadísticas de calificaciones

## 💡 Tips para la Presentación

### Antes de empezar:
```bash
# 1. Abre una terminal
cd /home/victorwkey/desktop/queries-big-data

# 2. Verifica que todo esté bien
./check-setup.sh

# 3. Ten listos ambos modos en terminales separadas
```

### Estructura sugerida:

1. **Introducción (1 min)**
   - Menciona que usas Scala + Spark
   - Muestra el dataset (85,855 películas)

2. **Consultas Predefinidas (3 min)**
   ```bash
   ./run-predefined.sh
   ```
   - Explica brevemente cada consulta mientras se ejecuta

3. **Demostración Interactiva (5 min)**
   ```bash
   ./run-interactive.sh
   ```
   - Ejecuta 2-3 consultas simples (opciones 1-9)
   - Muestra estadísticas (opción 11)
   - Si hay tiempo, una consulta SQL personalizada (opción 10)

4. **Consultas en Vivo del Profesor (5-10 min)**
   - Usa la opción 10 (SQL personalizado) para máxima flexibilidad
   - Ten la tabla de columnas disponible:
     - `title`, `year`, `genre`, `director`, `actors`
     - `avg_vote`, `votes`, `country`, `language`
     - `budget`, `usa_gross_income`, `worldwide_gross_income`

### Frases clave para tu presentación:

- "El dataset contiene **85,855 películas** de IMDB"
- "Usé **Apache Spark 3.3.1** con **Scala 2.12**"
- "Las consultas usan **DataFrames API** y **Spark SQL**"
- "El sistema soporta consultas en tiempo real gracias a Spark"

## 🔍 Columnas Disponibles en el Dataset

Para consultas SQL personalizadas:

**Tabla: `movies`**
- `imdb_title_id` - ID único de IMDB
- `title` - Título de la película
- `original_title` - Título original
- `year` - Año de lanzamiento
- `date_published` - Fecha de publicación
- `genre` - Género(s) (separados por coma)
- `duration` - Duración en minutos
- `country` - País(es) de producción
- `language` - Idioma(s)
- `director` - Director(es)
- `writer` - Guionista(s)
- `production_company` - Compañía de producción
- `actors` - Actores principales
- `description` - Sinopsis
- `avg_vote` - Calificación promedio (0-10)
- `votes` - Número de votos
- `budget` - Presupuesto
- `usa_gross_income` - Ingresos en USA
- `worldwide_gross_income` - Ingresos mundiales
- `metascore` - Puntuación de críticos
- `reviews_from_users` - Reviews de usuarios
- `reviews_from_critics` - Reviews de críticos

## 🆘 Solución de Problemas Rápida

**Si Spark no responde:**
```bash
# Ctrl + C para cancelar
# Reinicia con menos memoria si es necesario
```

**Si hay error de memoria:**
- El sistema ya está configurado con 4GB
- WSL normalmente tiene suficiente memoria

**Si la consulta tarda mucho:**
- Es normal la primera vez (carga de datos)
- Consultas posteriores son más rápidas

## 📊 Datos Interesantes para Mencionar

Si revisas las estadísticas (opción 11), encontrarás:
- Géneros más populares: Drama, Comedy, Action
- Mayor producción: USA, UK, France
- Películas desde 1894 hasta 2020+
- Idioma predominante: English

---

**¡Éxito en tu presentación! 🚀**

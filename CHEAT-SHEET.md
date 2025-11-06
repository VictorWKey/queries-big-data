# 🚀 CHEAT SHEET - Consultas Rápidas

## Comandos de Ejecución

```bash
# Verificar que todo esté listo
./check-setup.sh

# Ejecutar las 9 consultas predefinidas
./run-predefined.sh

# Modo interactivo (MEJOR para presentación)
./run-interactive.sh
```

## Menú Interactivo - Opciones

```
1.  Películas por intervalo de años
2.  Películas por puntuación IMDB mínima
3.  Películas por género
4.  Películas de dos géneros
5.  Películas con nombre similar
6.  Películas por años + género + calificación
7.  Películas por actor
8.  Películas por actor + años
9.  Películas por actor + años + género
10. Consulta SQL personalizada ⭐
11. Estadísticas generales ⭐
0.  Salir
```

## Consultas SQL Rápidas para Copiar/Pegar

### Top 10 Países Productores
```sql
SELECT country, COUNT(*) as total FROM movies WHERE country IS NOT NULL GROUP BY country ORDER BY total DESC LIMIT 10
```

### Top Películas Mejor Calificadas
```sql
SELECT title, year, avg_vote, votes FROM movies WHERE votes > 50000 ORDER BY avg_vote DESC LIMIT 20
```

### Películas de Sci-Fi
```sql
SELECT title, year, avg_vote, director FROM movies WHERE genre LIKE '%Sci-Fi%' AND votes > 10000 ORDER BY avg_vote DESC LIMIT 15
```

### Evolución por Década
```sql
SELECT FLOOR(year/10)*10 as decada, COUNT(*) as total, ROUND(AVG(avg_vote), 2) as promedio FROM movies WHERE year IS NOT NULL GROUP BY decada ORDER BY decada
```

### Películas Más Populares
```sql
SELECT title, year, avg_vote, votes, genre FROM movies ORDER BY votes DESC LIMIT 20
```

### Directores Más Prolíficos
```sql
SELECT director, COUNT(*) as peliculas, ROUND(AVG(avg_vote), 2) as promedio FROM movies WHERE director IS NOT NULL GROUP BY director ORDER BY peliculas DESC LIMIT 20
```

### Películas Recientes de Alta Calificación
```sql
SELECT title, year, avg_vote, genre, director FROM movies WHERE year >= 2015 AND votes > 10000 ORDER BY avg_vote DESC LIMIT 20
```

### Géneros Más Comunes
```sql
SELECT genre, COUNT(*) as total FROM movies WHERE genre IS NOT NULL GROUP BY genre ORDER BY total DESC LIMIT 15
```

## Columnas Principales

- `title` - Título
- `year` - Año
- `genre` - Género
- `director` - Director
- `actors` - Actores
- `avg_vote` - Calificación (0-10)
- `votes` - Número de votos
- `country` - País
- `language` - Idioma
- `duration` - Duración

## Actores/Directores Famosos para Demos

**Actores:**
- Leonardo DiCaprio
- Tom Hanks
- Robert Downey Jr.
- Brad Pitt
- Morgan Freeman
- Samuel L. Jackson
- Scarlett Johansson

**Directores:**
- Christopher Nolan
- Steven Spielberg
- Quentin Tarantino
- Martin Scorsese
- James Cameron

## Géneros Comunes

- Action
- Comedy
- Drama
- Horror
- Romance
- Sci-Fi
- Thriller
- Adventure
- Animation
- Crime
- Fantasy
- Mystery

## Tips Rápidos

1. **Primera consulta**: Siempre tarda más (carga de datos)
2. **Consultas siguientes**: Mucho más rápidas
3. **SQL personalizado**: Opción 10 del menú interactivo
4. **Estadísticas**: Opción 11 - impresiona mucho
5. **Ctrl+C**: Para cancelar si algo tarda mucho

## Datos Curiosos del Dataset

- Total películas: **85,855**
- Años: **1894 - 2020+**
- Países: **USA, UK, Francia, etc.**
- Idioma principal: **English**
- Géneros top: **Drama, Comedy, Action**

---
**Para cualquier consulta que pida el profesor, usa la Opción 10 (SQL personalizado)**

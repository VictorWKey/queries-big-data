## 🔍 FILTRAR FILAS (`filter` / `where`)

```scala
df.filter(col("columna") > 10)
df.where(col("columna") === "valor")
df.filter(col("columna").isNotNull)
df.filter(col("columna").isNull)
df.filter(col("texto").contains("Drama"))
df.filter(col("titulo").startsWith("The"))
df.filter(col("titulo").endsWith("Man"))
df.filter(col("año").between(2010, 2020))
df.filter(col("columna").isin(1, 2, 3))
df.filter(!(col("columna") === "valor"))  // Negación
````

🧠 **Combinar condiciones**

```scala
df.filter(
  (col("año") > 2015) && 
  (col("votos") > 10000) &&
  (col("genero").contains("Action") || col("genero").contains("Adventure"))
)
```

---

## 🎯 SELECCIONAR COLUMNAS (`select`, `drop`, `alias`)

```scala
df.select("titulo", "año", "puntuacion")
df.select(col("titulo"), col("puntuacion").alias("rating"))
df.drop("columna_innecesaria")
df.selectExpr("titulo as nombre", "año + 1 as año_siguiente")
```

---

## ⚙️ CREAR / MODIFICAR COLUMNAS (`withColumn`)

```scala
df.withColumn("decada", col("año") - (col("año") % 10))
df.withColumn("es_reciente", col("año") >= 2015)
df.withColumnRenamed("avg_vote", "promedio")
```

---

## 📊 AGRUPAR Y AGREGAR (`groupBy` + `agg`)

```scala
df.groupBy("genero").count()
df.groupBy("año").agg(avg("puntuacion").alias("promedio"))
df.groupBy("genero").agg(
  avg("puntuacion").alias("promedio"),
  count("*").alias("cantidad"),
  max("puntuacion").alias("maxima")
)
```

Funciones comunes:
`count`, `sum`, `avg`, `min`, `max`, `first`, `last`

---

## 🧱 ORDENAR RESULTADOS (`orderBy`, `sort`)

```scala
df.orderBy(col("puntuacion").desc)
df.orderBy(col("año").asc, col("puntuacion").desc)
df.sort(col("votos").desc)
```

---

## 🔗 UNIONES ENTRE DATAFRAMES (`join`)

```scala
df1.join(df2, Seq("id"), "inner")       // Coincidencias
df1.join(df2, Seq("id"), "left")        // Todo de izquierda
df1.join(df2, Seq("id"), "right")       // Todo de derecha
df1.join(df2, Seq("id"), "outer")       // Todo de ambos
df1.join(df2, Seq("id"), "left_semi")   // Solo las que coinciden
df1.join(df2, Seq("id"), "left_anti")   // Solo las que NO coinciden
```

📘 `Seq("id")` = lista de columnas por las que unir.

---

## 🧮 CONTAR, LIMPIAR Y DEDUPLICAR

```scala
df.count()                        // Número de filas
df.distinct()                     // Elimina duplicados
df.dropDuplicates(Seq("titulo"))  // Elimina duplicados por columnas
df.na.drop()                      // Elimina filas con null
df.na.fill(0, Seq("puntuacion"))  // Rellena null con 0 en esa columna
```

---

## 🔡 OPERACIONES DE TEXTO

```scala
df.filter(lower(col("titulo")).contains("dark"))
df.withColumn("titulo_mayus", upper(col("titulo")))
df.withColumn("longitud", length(col("titulo")))
df.filter(col("titulo").rlike("(?i)dark.*"))   // Regex (case-insensitive)
```

---

## 📈 LIMITE Y MUESTRA

```scala
df.limit(10)              // Primeras 10 filas
df.sample(0.1)            // 10% de las filas
df.head(5)                // 5 primeras filas (colección local)
```

---

```

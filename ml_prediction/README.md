# 🎬 IMDB Rating Prediction - Machine Learning con SparkML

Modelo de predicción de calificaciones IMDB usando Apache Spark y técnicas avanzadas de Feature Engineering.

---

## 📊 Resumen del Proyecto

**Objetivo:** Predecir la calificación promedio (`avg_vote`) de películas en IMDB usando features textuales y numéricas.

**Dataset:**
- 85,855 películas (dataset completo)
- 69,107 películas (después de limpieza - 80.49% retención)
- 70 columnas originales
- Target: `avg_vote` (escala 1.0-10.0, media ~5.9)

**Resultado Final:**
- 🏆 **Mejor modelo:** Gradient Boosted Trees
- 📈 **R² = 0.9945** (explica 99.45% de la varianza)
- 📉 **RMSE = 0.0912** (error promedio ~0.09 puntos)
- ⚡ **Tiempo total:** ~1.2 minutos

---

## 🔧 Arquitectura Técnica

### Features Engineering (~130 features total)

#### 1️⃣ **Features de Texto (100 features)**
- **TF-IDF Unigrams** en columna `description`
- Tokenización + StopWords removal
- HashingTF con 100 features
- IDF para capturar importancia semántica

#### 2️⃣ **Features Categóricas (19 features)**
- **Genre:** Feature Hashing (16 features)
  - Maneja 1,117 combinaciones únicas sin OutOfMemory
  - Reducción de cardinalidad: Top 30 + "Other"
  
- **Director:** Target Encoding (1 feature)
  - Reduce 10,000+ directores a 1 feature numérica
  - Smoothing factor = 10 para evitar overfitting
  
- **Actors:** Target Encoding (1 feature)
  - Reduce 55,457 actores a 1 feature numérica
  - Captura "prestigio" promedio del elenco
  
- **Duration Category:** StringIndexer (1 feature)
  - Categorías: short (≤90 min), medium (90-120), long (>120)

#### 3️⃣ **Features Numéricas (11 features)**

**Originales:**
- `duration` - Duración en minutos
- `votes` - Número de votos
- `reviews_from_users` - Reviews de usuarios
- `reviews_from_critics` - Reviews de críticos
- `year_clean` - Año de estreno (limpio)

**Derivadas (Feature Engineering):**
- `log_votes` = log₁(votes + 1) - Normaliza distribución
- `votes_per_review` = votes / (reviews_users + reviews_critics + 1)
- `review_ratio` = reviews_users / (reviews_critics + 1)
- `decade` = (year_clean / 10) × 10 - Agrupa por década
- `is_recent` = 1 si year ≥ 2015, else 0
- `is_old_classic` = 1 si year ≤ 1980, else 0

### Normalización
- **StandardScaler** aplicado a todas las features
- `withStd=true, withMean=false` (óptimo para sparse vectors)

---

## 🤖 Modelos Implementados

### 1. Ridge Regression (Baseline)
```scala
LinearRegression(
  maxIter = 100,
  regParam = 0.1,        // L2 regularization
  elasticNetParam = 0.0  // Pure Ridge
)
```
**Resultados:**
- RMSE: 0.1265
- R²: 0.9895
- Tiempo: 0.11 min

### 2. Random Forest Regressor
```scala
RandomForestRegressor(
  numTrees = 30,
  maxDepth = 8,
  minInstancesPerNode = 10,
  subsamplingRate = 0.8  // Optimización de memoria
)
```
**Resultados:**
- RMSE: 0.1702
- R²: 0.9810
- Tiempo: 0.24 min

### 3. Gradient Boosted Trees (🏆 Mejor)
```scala
GBTRegressor(
  maxIter = 50,
  maxDepth = 5,
  stepSize = 0.1,        // Learning rate
  subsamplingRate = 0.8
)
```
**Resultados:**
- RMSE: 0.0912
- R²: 0.9945
- Tiempo: 0.82 min

### 4. Ensemble Model
```scala
prediction_ensemble = 0.2×Ridge + 0.3×RF + 0.5×GBT
```
**Resultados:**
- RMSE: 0.0977
- R²: 0.9937
- Combina fortalezas de múltiples modelos

---

## 🚀 Ejecución

### Prerequisitos
- Apache Spark 3.3.1+
- Scala 2.12.15+
- 14GB+ RAM disponible

### Iniciar Spark Shell
```bash
spark-shell \
  --driver-memory 14g \
  --executor-memory 14g \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.2 \
  --conf spark.sql.shuffle.partitions=50 \
  --conf spark.driver.maxResultSize=2g
```

### Ejecutar Modelo
```scala
:load ml_prediction/IMDBPredictionModelSimplified.scala
IMDBPredictionModelSimplified.main(Array())
```

### Validar Datos (Opcional)
```scala
:load ml_prediction/DataValidation.scala
DataValidation.main(Array())
```

---

## 📁 Estructura del Proyecto

```
ml_prediction/
├── README.md                              # Este archivo
├── DataValidation.scala                   # Script de validación de datos
├── IMDBPredictionModelSimplified.scala   # 🎯 MODELO FINAL
└── resultados/
    ├── reporte_simplificado.txt          # Reporte comparativo
    ├── simplified_baseline_predictions.txt
    ├── simplified_rf_predictions.txt
    ├── simplified_gbt_predictions.txt
    └── simplified_ensemble_predictions.txt
```

---

## 💡 Técnicas Clave para Manejo de Memoria

### Problema Original
- **OutOfMemoryError** con Random Forest debido a alta cardinalidad de features categóricas
- `director` (10,000+ valores), `actors` (55,457 valores), `genre` (1,117 valores)
- StringIndexer + OneHotEncoder creaban features explosivas

### Soluciones Implementadas

#### 1️⃣ **Target Encoding con Smoothing**
```scala
encoded_value = (category_mean × count + global_mean × 10) / (count + 10)
```
- Reduce cualquier cardinalidad a 1 feature numérica
- Captura "valor promedio" de la categoría
- Smoothing evita overfitting en categorías raras

#### 2️⃣ **Feature Hashing**
```scala
FeatureHasher(numFeatures = 16)
```
- Dimensión fija independiente de cardinalidad
- No requiere mantener diccionarios en memoria
- Tolerante a colisiones con hash functions

#### 3️⃣ **Reducción de Cardinalidad**
```scala
top30_genres + "Other"
```
- Agrupa categorías raras en "Other"
- Mantiene información de categorías frecuentes

#### 4️⃣ **Subsampling**
```scala
subsamplingRate = 0.8
```
- Cada árbol/iteración usa solo 80% de datos
- Reduce memoria y mejora generalización

---

## 📈 Resultados Comparativos

| Modelo | RMSE ↓ | MAE ↓ | R² ↑ | Tiempo | Mejora vs Baseline |
|--------|--------|-------|------|--------|-------------------|
| **GBT** | **0.0912** | **0.0401** | **0.9945** | 0.82 min | **27.95%** |
| Ensemble | 0.0977 | 0.0555 | 0.9937 | ~1 min | 22.77% |
| Ridge | 0.1265 | 0.0951 | 0.9895 | 0.11 min | - |
| Random Forest | 0.1702 | 0.1025 | 0.9810 | 0.24 min | -34.55% |

---

## 🎓 Lecciones Aprendidas

### ✅ Funciona Bien
1. **Target Encoding** para high-cardinality categoricals
2. **Feature Hashing** para control de memoria
3. **TF-IDF** captura información semántica de texto
4. **Feature Engineering** (log_votes, ratios) mejora significativamente
5. **GBT** supera a Random Forest en este dataset

### ⚠️ Limitaciones Identificadas
1. Random Forest requiere más memoria con features categóricas
2. Grid Search con CrossValidation es prohibitivo en tiempo
3. Bi-gramas (n-grams=2) no justifican el costo computacional
4. OneHotEncoder explota con alta cardinalidad

### 🚀 Posibles Mejoras Futuras
1. Word2Vec en lugar de TF-IDF para semántica profunda
2. Análisis de sentimiento en `description`
3. Features de presupuesto/recaudación (si disponibles)
4. Información de premios y nominaciones
5. Tuning fino de hiperparámetros (requiere cluster)

---

## 📊 Feature Importance (Top 10 - GBT)

*Los índices de features están en el vector assembler final*

1. **Feature 51** (29.78%) - Probablemente `log_votes`
2. **Feature 52** (25.89%) - Probablemente `votes`
3. **Feature 54** (19.57%) - Features numéricas derivadas
4. **Feature 50** (17.69%) - TF-IDF components
5. **Feature 53** (5.14%) - Categorical encoded
6. Resto < 1% cada una

**Conclusión:** Features numéricas (votes, reviews) son las más predictivas, seguidas de text features (description).

---

## 👨‍💻 Autor

**Victor W. Key**
- Dataset: IMDB Movies Extensive Dataset
- Framework: Apache Spark 3.3.1 + SparkML
- Fecha: Noviembre 2025

---

## 📄 Licencia

Proyecto educativo - Análisis de Big Data con Spark

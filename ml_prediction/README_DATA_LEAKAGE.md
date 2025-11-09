# 🚨 DETECCIÓN Y CORRECCIÓN DE DATA LEAKAGE

## 📌 RESUMEN EJECUTIVO

Se detectó **data leakage crítico** en el modelo original que inflaba artificialmente las métricas hasta **R² = 0.88**. El problema radica en el uso de variables (`votes`, `reviews_from_users`, `reviews_from_critics`) que son **consecuencias posteriores** al rating, violando la causalidad temporal.

---

## 🔴 PROBLEMA IDENTIFICADO

### Variables con Data Leakage:
- ❌ `votes` - Popularidad correlacionada con rating
- ❌ `log_votes` - Derivado de votes
- ❌ `reviews_from_users` - Posterior al rating
- ❌ `reviews_from_critics` - Posterior al rating
- ❌ `votes_per_review` - Derivado de variables contaminadas
- ❌ `review_ratio` - Derivado de variables contaminadas

### Impacto:
- **R² inflado artificialmente** a 0.80-0.90
- Modelo **NO generaliza** a películas nuevas sin historial
- Viola principios de **causalidad temporal**

---

## ✅ SOLUCIÓN IMPLEMENTADA

### Archivos Creados:

1. **`IMDBPredictionModelNOLEAKAGE.scala`**
   - Modelo corregido sin data leakage
   - Solo usa características intrínsecas
   - R² esperado: 0.20-0.40 (realista)

2. **`VerificarDataLeakage.scala`**
   - Script de diagnóstico
   - Analiza correlaciones
   - Detecta variables problemáticas

3. **`ANALISIS_DATA_LEAKAGE.md`**
   - Documentación técnica detallada
   - Explicación del problema
   - Referencias teóricas

4. **`comparar_modelos.sh`**
   - Script bash para ejecutar comparación
   - Ejecuta ambos modelos
   - Muestra diferencias en métricas

---

## 🚀 CÓMO EJECUTAR

### Opción 1: Verificar Data Leakage (Recomendado primero)

```bash
# En Spark Shell
spark-shell --driver-memory 8g

# Cargar script de verificación
:load ml_prediction/VerificarDataLeakage.scala
VerificarDataLeakage.main(Array())
:quit
```

**Salida esperada:**
- Correlaciones de cada variable con `avg_vote`
- Distribución de votos/reviews por rating
- Confirmación de data leakage

---

### Opción 2: Ejecutar Modelo Corregido

```bash
# En Spark Shell
spark-shell --driver-memory 10g

# Cargar modelo SIN data leakage
:load ml_prediction/IMDBPredictionModelNOLEAKAGE.scala
IMDBPredictionModelNOLEAKAGE.main(Array())
:quit
```

**Salida esperada:**
- R² entre 0.20 y 0.40
- RMSE entre 0.60 y 0.80
- Predicción basada SOLO en características intrínsecas

---

### Opción 3: Comparar Ambos Modelos (Para evidenciar el problema)

```bash
# Dar permisos de ejecución
chmod +x ml_prediction/comparar_modelos.sh

# Ejecutar comparación
./ml_prediction/comparar_modelos.sh
```

**Advertencia:** Esto ejecutará ambos modelos secuencialmente, puede tomar ~30-60 minutos.

---

## 📊 RESULTADOS ESPERADOS

### Modelo ORIGINAL (CON data leakage):
```
Ridge Regression:  R² = 0.75-0.80
Random Forest:     R² = 0.82-0.85
GBT:               R² = 0.85-0.88
Ensemble:          R² = 0.87-0.90
```

### Modelo CORREGIDO (SIN data leakage):
```
Ridge Regression:  R² = 0.15-0.25
Random Forest:     R² = 0.25-0.35
GBT:               R² = 0.30-0.40
Ensemble:          R² = 0.35-0.45
```

### ⚠️ Interpretación:
- **Caída dramática en R²** → CONFIRMA data leakage original
- **R² bajo NO es malo** → Refleja la dificultad real del problema
- Ratings de IMDB son **subjetivos** y difíciles de predecir

---

## 📁 ARCHIVOS GENERADOS

Después de ejecutar los modelos, encontrarás:

```
ml_prediction/resultados/
├── reporte_simplificado.txt         # Modelo ORIGINAL (con leakage)
├── reporte_noleakage.txt            # Modelo CORREGIDO (sin leakage)
├── simplified_baseline_predictions.txt
├── simplified_rf_predictions.txt
├── simplified_gbt_predictions.txt
├── simplified_ensemble_predictions.txt
├── noleakage_baseline_predictions.txt
├── noleakage_rf_predictions.txt
├── noleakage_gbt_predictions.txt
└── noleakage_ensemble_predictions.txt
```

---

## 🔍 VALIDACIÓN ADICIONAL

### Test 1: Análisis de Correlaciones

```scala
// En Spark Shell
:load ml_prediction/VerificarDataLeakage.scala
VerificarDataLeakage.main(Array())
```

Si `corr(avg_vote, votes) > 0.5` → **CONFIRMA data leakage**

### Test 2: Inspección de Feature Importance

Revisa los reportes generados. Si en el modelo original `votes` está en el Top 5 de importancia → **LEAKAGE CONFIRMADO**

### Test 3: Distribución de Votos por Rating

El script de verificación mostrará:
```
Rango de Rating      Promedio Votos
----------------------------------
Excelente (8+)       ~200,000
Bueno (7-8)          ~80,000
Medio (6-7)          ~30,000
Bajo (5-6)           ~15,000
```

Esta correlación directa **confirma el leakage**.

---

## 🎯 PRÓXIMOS PASOS RECOMENDADOS

### Fase 1: Diagnóstico (AHORA)
1. ✅ Ejecutar `VerificarDataLeakage.scala`
2. ✅ Revisar correlaciones y distribuciones
3. ✅ Confirmar presencia de data leakage

### Fase 2: Corrección (AHORA)
4. ✅ Ejecutar `IMDBPredictionModelNOLEAKAGE.scala`
5. ✅ Comparar métricas con modelo original
6. ✅ Documentar diferencias en reporte

### Fase 3: Mejoras Futuras (OPCIONAL)
7. 📈 Implementar BERT embeddings para description
8. 🧠 Agregar features de red (colaboraciones director-actor)
9. 💰 Incorporar presupuesto y premios (si disponible)
10. 🔄 Cross-validation 5-fold para robustez

---

## 📚 REFERENCIAS

### Data Leakage en Machine Learning:
- Kaufman et al. (2012): "Leakage in Data Mining"
- Kapoor & Narayanan (2022): "Leakage and the Reproducibility Crisis in ML-based Science"
- Hastie et al. (2009): "The Elements of Statistical Learning" - Cap. 7

### Causalidad y Predicción:
- Pearl (2009): "Causality: Models, Reasoning and Inference"
- Schölkopf et al. (2021): "Toward Causal Representation Learning"

---

## ❓ FAQ

### P: ¿Por qué el R² bajó tanto?
**R:** Porque el modelo original hacía "cheating" usando variables que solo existen después del rating. El R² bajo es **realista** para este problema.

### P: ¿Un R² de 0.30 es aceptable?
**R:** SÍ. Para predicción de ratings subjetivos usando solo características intrínsecas, es **excelente**. Muchos papers académicos reportan R² similares.

### P: ¿Puedo usar `votes` si lo normalizo?
**R:** NO. Normalizar no elimina el data leakage. La variable sigue siendo posterior al rating.

### P: ¿Y si uso `votes` de películas similares?
**R:** Depende. Si usas votos históricos de otras películas del mismo director/género, puede ser válido. Pero requiere **feature engineering cuidadoso**.

### P: ¿Cómo mejoro el R² sin hacer cheating?
**R:** 
1. Text embeddings avanzados (BERT, GPT)
2. Features de red (colaboraciones)
3. Datos externos (presupuesto, premios)
4. Ensembles más sofisticados
5. Feature engineering creativo

---

## 📞 SOPORTE

Si tienes dudas o encuentras problemas:
1. Revisa `ANALISIS_DATA_LEAKAGE.md` (documentación técnica)
2. Ejecuta `VerificarDataLeakage.scala` para diagnóstico
3. Compara métricas entre modelos original y corregido

---

**Última actualización:** 2025-11-09  
**Archivo principal:** `IMDBPredictionModelNOLEAKAGE.scala`  
**Diagnóstico:** `VerificarDataLeakage.scala`  
**Documentación:** `ANALISIS_DATA_LEAKAGE.md`

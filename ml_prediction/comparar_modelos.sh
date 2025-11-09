#!/bin/bash

# Script para ejecutar ambos modelos y comparar resultados
# Evidencia de data leakage en el modelo original

echo "========================================="
echo "🧪 EXPERIMENTO: COMPARACIÓN DE MODELOS"
echo "========================================="
echo ""
echo "Este script ejecutará:"
echo "  1. Modelo ORIGINAL (CON data leakage)"
echo "  2. Modelo CORREGIDO (SIN data leakage)"
echo ""
echo "Expectativa:"
echo "  - Original: R² ≈ 0.80-0.90 (SOSPECHOSO)"
echo "  - Corregido: R² ≈ 0.20-0.40 (REALISTA)"
echo ""
echo "========================================="
echo ""

# Verificar que Spark esté disponible
if ! command -v spark-shell &> /dev/null; then
    echo "❌ ERROR: spark-shell no encontrado"
    echo "   Instala Apache Spark o configura PATH"
    exit 1
fi

# Crear directorio de resultados
mkdir -p ml_prediction/resultados

# Ejecutar modelo ORIGINAL
echo "📊 PASO 1: Ejecutando modelo ORIGINAL (con data leakage)..."
echo "==========================================="

spark-shell --driver-memory 10g <<'EOF'
:load ml_prediction/IMDBPredictionModelSimplified.scala
IMDBPredictionModelSimplified.main(Array())
:quit
EOF

echo ""
echo "✅ Modelo original completado"
echo ""

# Ejecutar modelo CORREGIDO
echo "📊 PASO 2: Ejecutando modelo CORREGIDO (sin data leakage)..."
echo "==========================================="

spark-shell --driver-memory 10g <<'EOF'
:load ml_prediction/IMDBPredictionModelNOLEAKAGE.scala
IMDBPredictionModelNOLEAKAGE.main(Array())
:quit
EOF

echo ""
echo "✅ Modelo corregido completado"
echo ""

# Comparar resultados
echo "========================================="
echo "📈 COMPARACIÓN DE RESULTADOS"
echo "========================================="
echo ""

if [ -f ml_prediction/resultados/reporte_simplificado.txt ]; then
    echo "🔴 MODELO ORIGINAL (CON data leakage):"
    echo "---------------------------------------"
    grep -A 10 "RESULTADOS:" ml_prediction/resultados/reporte_simplificado.txt | head -15
    echo ""
fi

if [ -f ml_prediction/resultados/reporte_noleakage.txt ]; then
    echo "✅ MODELO CORREGIDO (SIN data leakage):"
    echo "---------------------------------------"
    grep -A 10 "RESULTADOS:" ml_prediction/resultados/reporte_noleakage.txt | head -15
    echo ""
fi

echo "========================================="
echo "📊 ANÁLISIS DE DIFERENCIAS"
echo "========================================="

# Extraer R² de ambos reportes
if [ -f ml_prediction/resultados/reporte_simplificado.txt ] && [ -f ml_prediction/resultados/reporte_noleakage.txt ]; then
    echo ""
    echo "Archivos de resultados generados:"
    echo "  - ml_prediction/resultados/reporte_simplificado.txt"
    echo "  - ml_prediction/resultados/reporte_noleakage.txt"
    echo ""
    echo "Revisa los archivos para ver las métricas completas."
    echo ""
    echo "⚠️  Si el modelo ORIGINAL tiene R² > 0.70:"
    echo "   → CONFIRMA data leakage por uso de votes/reviews"
    echo ""
    echo "✅ Si el modelo CORREGIDO tiene R² < 0.50:"
    echo "   → Es REALISTA - predicción genuina sin cheating"
    echo ""
fi

echo "========================================="
echo "✅ EXPERIMENTO COMPLETADO"
echo "========================================="

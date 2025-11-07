#!/bin/bash

# Script de verificacion rapida

echo "Verificando el sistema para Consultas IMDB con Spark"

# Verificar Spark
echo "Verificando Apache Spark..."
if command -v spark-shell &> /dev/null; then
    echo "Spark instalado"
    spark-shell --version 2>&1 | head -5
else
    echo "Spark no encontrado. Ejecuta: source ~/.zshrc"
    exit 1
fi

echo ""
echo "Verificando archivos del dataset..."
if [ -f "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv" ]; then
    echo "IMDb movies.csv encontrado"
else
    echo "IMDb movies.csv NO encontrado"
    exit 1
fi

if [ -f "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb ratings.csv" ]; then
    echo "IMDb ratings.csv encontrado"
else
    echo "IMDb ratings.csv NO encontrado"
    exit 1
fi

echo ""
echo "Verificando scripts de Scala..."
if [ -f "IMDBQueriesCompact.scala" ]; then
    echo "IMDBQueriesCompact.scala encontrado"
else
    echo "IMDBQueriesCompact.scala NO encontrado"
    exit 1
fi

echo ""
echo "Todo listo para ejecutar"


#!/bin/bash

# Script de inicio rápido - Verifica la instalación y hace una prueba rápida

echo "==================================================================="
echo "  VERIFICACIÓN DEL SISTEMA - Consultas IMDB con Spark"
echo "==================================================================="
echo ""

# Verificar Spark
echo "1. Verificando Apache Spark..."
if command -v spark-shell &> /dev/null; then
    echo "   ✓ Spark instalado correctamente"
    spark-shell --version 2>&1 | head -5
else
    echo "   ✗ Spark no encontrado. Ejecuta: source ~/.zshrc"
    exit 1
fi

echo ""
echo "2. Verificando archivos del dataset..."
if [ -f "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb movies.csv" ]; then
    echo "   ✓ IMDb movies.csv encontrado"
else
    echo "   ✗ IMDb movies.csv NO encontrado"
    exit 1
fi

if [ -f "IMDB-Movies-Extensive-Dataset-Analysis/data1/IMDb ratings.csv" ]; then
    echo "   ✓ IMDb ratings.csv encontrado"
else
    echo "   ✗ IMDb ratings.csv NO encontrado"
    exit 1
fi

echo ""
echo "3. Verificando scripts de Scala..."
if [ -f "IMDBQueries.scala" ]; then
    echo "   ✓ IMDBQueries.scala encontrado"
else
    echo "   ✗ IMDBQueries.scala NO encontrado"
    exit 1
fi

if [ -f "IMDBInteractive.scala" ]; then
    echo "   ✓ IMDBInteractive.scala encontrado"
else
    echo "   ✗ IMDBInteractive.scala NO encontrado"
    exit 1
fi

echo ""
echo "==================================================================="
echo "  ✓ TODO LISTO PARA EJECUTAR"
echo "==================================================================="
echo ""
echo "Opciones disponibles:"
echo ""
echo "  1. Consultas predefinidas:  ./run-predefined.sh"
echo "  2. Modo interactivo:        ./run-interactive.sh"
echo ""
echo "==================================================================="

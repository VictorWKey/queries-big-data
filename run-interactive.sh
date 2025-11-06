#!/bin/bash

# Script para ejecutar el modo interactivo de consultas IMDB

cd /home/victorwkey/desktop/queries-big-data

echo "==================================================================="
echo "Modo Interactivo - Consultas IMDB con Spark"
echo "==================================================================="
echo ""

/opt/spark/bin/spark-shell -i IMDBInteractive.scala

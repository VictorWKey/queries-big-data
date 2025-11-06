#!/bin/bash

# Script para ejecutar las consultas predefinidas de IMDB

cd /home/victorwkey/desktop/queries-big-data

echo "==================================================================="
echo "Ejecutando consultas predefinidas de IMDB con Spark"
echo "==================================================================="
echo ""

/opt/spark/bin/spark-shell -i IMDBQueries.scala

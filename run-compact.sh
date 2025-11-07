#!/bin/bash

# Script para ejecutar consultas IMDB compactas

cd /home/victorwkey/queries-big-data

echo "Iniciando Spark..."

# Compilar y ejecutar el código Scala
/opt/spark/bin/spark-shell --conf spark.ui.showConsoleProgress=false <<'EOF' 2>&1 | grep -v "WARN\|INFO" | grep -v "^$"
:load IMDBQueriesCompact.scala
IMDBQueriesCompact.main(Array())
EOF

echo ""
echo "Ejecucion completada"

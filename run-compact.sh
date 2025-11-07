#!/bin/bash

# Script mejorado con salida compacta y limpia

cd /home/victorwkey/queries-big-data

clear
echo ""
echo "╔════════════════════════════════════════════════════════════════════════════════╗"
echo "║                     CONSULTAS IMDB - VERSIÓN COMPACTA                          ║"
echo "╚════════════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Iniciando Spark (esto puede tomar unos segundos)..."
echo ""

# Compilar y ejecutar el código Scala
/opt/spark/bin/spark-shell --conf spark.ui.showConsoleProgress=false <<'EOF' 2>&1 | grep -v "WARN\|INFO" | grep -v "^$" 
:load IMDBQueriesCompact.scala
IMDBQueriesCompact.main(Array())
EOF

echo ""
echo "╔════════════════════════════════════════════════════════════════════════════════╗"
echo "║                              EJECUCIÓN COMPLETADA                              ║"
echo "╚════════════════════════════════════════════════════════════════════════════════╝"
echo ""

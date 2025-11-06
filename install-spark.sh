#!/bin/bash

# Script de instalación de Apache Spark para WSL

echo "=== Configurando Apache Spark ==="

# Mover Spark a una ubicación estándar
if [ ! -d "/opt/spark" ]; then
    sudo mv ~/spark-3.3.1-bin-hadoop3 /opt/spark
    echo "✓ Spark movido a /opt/spark"
else
    echo "✓ Spark ya está en /opt/spark"
fi

# Configurar variables de entorno
SPARK_ENV_FILE="$HOME/.zshrc"

# Verificar si ya están configuradas las variables
if ! grep -q "SPARK_HOME" "$SPARK_ENV_FILE"; then
    echo "" >> "$SPARK_ENV_FILE"
    echo "# Apache Spark Configuration" >> "$SPARK_ENV_FILE"
    echo "export SPARK_HOME=/opt/spark" >> "$SPARK_ENV_FILE"
    echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> "$SPARK_ENV_FILE"
    echo "export PYSPARK_PYTHON=python3" >> "$SPARK_ENV_FILE"
    echo "✓ Variables de entorno agregadas a ~/.zshrc"
else
    echo "✓ Variables de entorno ya configuradas"
fi

# Aplicar las variables de entorno en la sesión actual
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

echo ""
echo "=== Instalación completada ==="
echo "Por favor ejecuta: source ~/.zshrc"
echo "Luego verifica con: spark-shell --version"

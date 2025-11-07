#!/bin/bash

# Script de instalacion de Apache Spark para WSL

echo "Configurando Apache Spark"

# Verificar si Spark ya está instalado
if [ -d "/opt/spark" ]; then
    echo "Spark ya esta instalado en /opt/spark"
else
    echo "Descargando Apache Spark 3.3.1..."
    
    # Descargar Spark si no existe
    SPARK_VERSION="3.3.1"
    HADOOP_VERSION="3"
    SPARK_DIR="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
    SPARK_TGZ="${SPARK_DIR}.tgz"
    SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"
    
    cd ~
    
    # Descargar si no existe el archivo
    if [ ! -f "$SPARK_TGZ" ]; then
        echo "Descargando desde: $SPARK_URL"
        wget -q --show-progress "$SPARK_URL"
        if [ $? -ne 0 ]; then
            echo "Error al descargar Spark. Verifica tu conexion a internet."
            exit 1
        fi
    else
        echo "Archivo $SPARK_TGZ ya existe"
    fi
    
    # Extraer el archivo
    if [ ! -d "$SPARK_DIR" ]; then
        echo "Extrayendo Spark..."
        tar -xzf "$SPARK_TGZ"
        if [ $? -ne 0 ]; then
            echo "Error al extraer Spark"
            exit 1
        fi
    fi
    
    # Mover a /opt/spark
    echo "Instalando Spark en /opt/spark..."
    sudo mv "$SPARK_DIR" /opt/spark
    
    # Limpiar archivo descargado
    rm -f "$SPARK_TGZ"
    
    echo "Spark instalado en /opt/spark"
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
    echo "Variables de entorno agregadas a ~/.zshrc"
else
    echo "Variables de entorno ya configuradas"
fi

# Aplicar las variables de entorno en la sesión actual
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

echo ""
echo "Instalacion completada"
echo "Ejecuta: source ~/.zshrc"
echo "Luego verifica con: spark-shell --version"

from datetime import datetime, timedelta
from delta import *
from faker import Faker
from pathlib import Path
import os
import pandas as pd
import pyspark
import pyspark.sql.functions as F
import random

BASE_DIR = str(Path.cwd().parent.parent)
DATALAKE_PATH = f"{BASE_DIR}/datalake"
BRONZE_PATH = f"{DATALAKE_PATH}/bronze"
SILVER_PATH = f"{DATALAKE_PATH}/silver"
GOLD_PATH = f"{DATALAKE_PATH}/gold"

builder = pyspark.sql.SparkSession.builder.appName("Projeto_2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# TB_GD_RESUMO_FRETES_PAGOS
def create_gold_freight_table(data_processamento: str) -> None:
    caminho_tabela_gold = f"{GOLD_PATH}/logistica/tb_gd_resumo_fretes_pagos"
    RECRIAR_TABELA = False

    try:
        df_resumo_fretes = DeltaTable.forPath(spark, caminho_tabela_gold)
        df_resumo_fretes.toDF().limit(1)

    except Exception as e:
        if 'DELTA_MISSING_DELTA_TABLE' in str(e):
            print('Tabela Delta não encontrada na camada prata. Realizando a criação de uma tabela nova.')
            RECRIAR_TABELA = True

        else:
            raise e

    if RECRIAR_TABELA:
        df_fretes = spark.read.format('delta').load(f"{SILVER_PATH}/frete/frete")
    else:
        df_fretes = spark.read.format('delta').load(f"{SILVER_PATH}/frete/frete").filter(f"data_envio = '{data_processamento}'")

    df_fretes.createOrReplaceTempView('fretes')

    df_fretes_sumarizado = spark.sql(
        """
        SELECT 
            DATA_ENVIO AS CodigoData,
            FILIAL_ID as FilialId,
            transportadora as Transportadora,
            ROUND(SUM(VALOR_FRETE), 4) AS TotalFretePago
        FROM 
            fretes
        GROUP BY 
            DATA_ENVIO,
            FILIAL_ID,
            transportadora
        ORDER BY 
            DATA_ENVIO, 
            FILIAL_ID, 
            TRANSPORTADORA
        """
    )

    print('Escrevendo tabela na camada gold em: ', caminho_tabela_gold)
    if RECRIAR_TABELA:
        (
            df_fretes_sumarizado
            .write
            .format('delta')
            .option('mergeSchema', 'true')
            .mode('overwrite')
            .save(caminho_tabela_gold)
        )
    else:
        df_resumo_fretes.delete(f"CodigoData = '{data_processamento}'")

        (
            df_fretes_sumarizado
            .write
            .format('delta')
            .option('mergeSchema', 'true')
            .mode('append')
            .save(caminho_tabela_gold)
        )

create_gold_freight_table('2024-04-21')

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

# TB_GOLD_TRIBUTARIO
def create_gold_taxes_table(data_processamento: str) -> None:
    caminho_tabela_gold = f"{GOLD_PATH}/tributario/tb_gd_resumo_impostos_pagos"
    RECRIAR_TABELA = False

    try:
        df_resumo_impostos = DeltaTable.forPath(spark, caminho_tabela_gold)
        df_resumo_impostos.toDF().limit(1)

    except Exception as e:
        if 'DELTA_MISSING_DELTA_TABLE' in str(e):
            print('Tabela Delta não encontrada na camada prata. Realizando a criação de uma tabela nova.')
            RECRIAR_TABELA = True

        else:
            raise e

    if RECRIAR_TABELA:
        df_tributos = spark.read.format('delta').load(f"{SILVER_PATH}/tributos/tributos")
    else:
        df_tributos = spark.read.format('delta').load(f"{SILVER_PATH}/tributos/tributos").filter(f"data_pagamento = '{data_processamento}'")

    df_tributos_sumarizado = (
        df_tributos
        .groupBy(
            F.col('data_pagamento').alias('CodigoData'),
            F.col('filial_id').alias('FilialId'),
            F.col('tipo_imposto').alias('TipoImposto'),
        )
        .agg(
            F.round(
                F.sum(F.col('valor_pago'))
            , 4).alias('TotalImpostoPago')
        )
        .orderBy('CodigoData', 
                 'FilialId', 
                 'TipoImposto', 
                )
    )

    print('Escrevendo tabela na camada gold em: ', caminho_tabela_gold)
    if RECRIAR_TABELA:
        (
            df_tributos_sumarizado
            .write
            .format('delta')
            .option('mergeSchema', 'true')
            .mode('overwrite')
            .save(caminho_tabela_gold)
        )
    else:
        df_resumo_impostos.delete(f"CodigoData = '{data_processamento}'")

        (
            df_tributos_sumarizado
            .write
            .format('delta')
            .option('mergeSchema', 'true')
            .mode('append')
            .save(caminho_tabela_gold)
        )

# create_gold_taxes_table('2024-04-21')
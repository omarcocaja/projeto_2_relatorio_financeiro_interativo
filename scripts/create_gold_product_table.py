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

def create_gold_product_table(data_processamento: str) -> None:
    caminho_tabela_gold = f"{GOLD_PATH}/vendas/tb_gd_resumo_produtos_filiais"
    RECRIAR_TABELA = False
    
    try:
        df_resumo_produtos = DeltaTable.forPath(spark, caminho_tabela_gold)
        df_resumo_produtos.toDF().limit(1)

    except Exception as e:
        if 'DELTA_MISSING_DELTA_TABLE' in str(e):
            print('Tabela Delta não encontrada na camada prata. Realizando a criação de uma tabela nova.')
            RECRIAR_TABELA = True

        else:
            raise e

    if RECRIAR_TABELA:
        df_devolucao = spark.read.format('delta').load(f"{SILVER_PATH}/devolucao/devolucao")
        df_estoque = spark.read.format('delta').load(f"{SILVER_PATH}/estoque/estoque")
        df_vendas = spark.read.format('delta').load(f"{SILVER_PATH}/vendas/vendas")
    else:
        df_devolucao = spark.read.format('delta').load(f"{SILVER_PATH}/devolucao/devolucao").filter(f"data_devolucao = '{data_processamento}'")
        df_estoque = spark.read.format('delta').load(f"{SILVER_PATH}/estoque/estoque").filter(f"data_referencia = '{data_processamento}'")
        df_vendas = spark.read.format('delta').load(f"{SILVER_PATH}/vendas/vendas").filter(f"data_venda = '{data_processamento}'")


    # SUMARIZAÇÃO DEVOLUCAO
    df_devolucao_sumarizado = (
        df_devolucao
        .groupBy(
            F.col('data_devolucao').alias('CodigoData'),
            F.col('produto_id').alias('ProdutoId'),
            F.col('filial_id').alias('FilialId')
        )
        .agg(
            F.sum(F.col('quantidade')).alias('TotalDevolvido')
        )      
        .orderBy('CodigoData', 'ProdutoId','FilialId')
    )
    
    # SUMARIZAÇÃO ESTOQUE
    df_estoque_sumarizado = (
        df_estoque
        .groupBy(
            F.col('data_referencia').alias('CodigoData'),
            F.col('produto_id').alias('ProdutoId'),
            F.col('filial_id').alias('FilialId')
        )
        .agg(
            F.sum(F.col('quantidade_disponivel')).alias('TotalEmEstoque')
        )      
        .orderBy('CodigoData', 'ProdutoId','FilialId')
    )

    # SUMARIZAÇÃO VENDAS
    df_vendas_sumarizado = (
        df_vendas
        .groupBy(
            F.col('data_venda').alias('CodigoData'),
            F.col('produto_id').alias('ProdutoId'),
            F.col('filial_id').alias('FilialId')
        )
        .agg(
            F.sum(F.col('quantidade')).alias('TotalQuantidadeVendida'),
            F.round(
                F.sum(F.col('valor_unitario'))
            , 2).alias('TotalValorVendido'),
            F.round(
                F.sum(F.col('valor_unitario')) / F.sum(F.col('quantidade'))
            , 2).alias('TicketMedio')
        )      
        .orderBy('CodigoData', 'ProdutoId','FilialId')
    )

    df_resumo_produtos_new = (
        df_devolucao_sumarizado
        .join(df_estoque_sumarizado, ['CodigoData', 'ProdutoId', 'FilialId'], 'inner')
        .join(df_vendas_sumarizado, ['CodigoData', 'ProdutoId', 'FilialId'], 'inner')
        .orderBy('CodigoData', 'ProdutoId','FilialId')
    )

    df_resumo_produtos_new.show(5, False)
    
    if RECRIAR_TABELA:
        (
            df_resumo_produtos_new
            .write
            .format('delta')
            .option('mergeSchema', 'true')
            .mode('overwrite')
            .save(caminho_tabela_gold)
        )
    else:
        df_resumo_produtos.delete(f"CodigoData = '{data_processamento}'")

        (
            df_resumo_produtos_new
            .write
            .format('delta')
            .option('mergeSchema', 'true')
            .mode('append')
            .save(caminho_tabela_gold)
        )

# create_gold_product_table('2024-04-21')
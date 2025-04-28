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

# Bronze para silver sistema de devolucao
def ingest_bronze_to_silver(
          sistema:str, 
          dominio:str, 
          nome_tabela:str, 
          prefixo:str, 
          data_processamento:str, 
          coluna_date:str,
          coluna_id:str,
          append_only:bool = False
        ) -> None:
        """Realiza a ingestão de dados da camada bronze para a camada silver/prata.

        Args:
            sistema (str): nome do sistema na camada bronze
            dominio (str): nome do domínio na camada silver
            nome_tabela (str): nome da tabela em ambas as camadas
            prefixo (str): prefixo do arquivo à ser lido
            data_processamento (str): data a ser processada
            coluna_date (str): nome da coluna de data para comparação na ingestão
            coluna_id (str): nome da coluna ID única

        Raises:
            e: se a tabela não existir na camada prata

        Returns:
            None
        """
        
        caminho_tabela_bronze = f"{BRONZE_PATH}/{sistema}/{nome_tabela}/{prefixo}_{data_processamento.replace('-', '_')}.csv"
        print('Lendo tabela bronze no caminho: ',caminho_tabela_bronze)
        
        caminho_tabela_prata = f"{SILVER_PATH}/{dominio}/{nome_tabela}"
        print('Caminho na camada prata à ser escrita a tabela: ', caminho_tabela_prata)

        df_bronze = spark.read.option('header', 'true').option('inferSchema', 'true').csv(caminho_tabela_bronze)

        try:
            df_silver = DeltaTable.forPath(spark, caminho_tabela_prata)
            df_silver.toDF().limit(1)
            
            if not append_only:
                (
                    df_silver.alias('old_data')
                    .merge(
                        df_bronze.alias('new_data'),
                        f"old_data.{coluna_id} = new_data.{coluna_id}"
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                df_silver.delete(f"{coluna_date} = '{data_processamento}'")
                
                (
                    df_bronze
                    .write
                    .format('delta')
                    .option('mergeSchema', 'true')
                    .mode('append')
                    .save(caminho_tabela_prata)
                )

        
        except Exception as e:
            if 'DELTA_MISSING_DELTA_TABLE' in str(e):
                print('Tabela Delta não encontrada na camada prata. Realizando a criação de uma tabela nova.')
                (
                    df_bronze
                    .write
                    .format('delta')
                    .option('mergeSchema', 'true')
                    .mode('overwrite')
                    .save(caminho_tabela_prata)
                )
            else:
                raise e

"""
Exemplos de uso para criação das tabelas de devolução, estoque, vendas, impostos e frete:

    DATA_PROCESSAMENTO = '2025-04-22'

    ingest_bronze_to_silver(
        sistema = 'sistema_devolucao',
        dominio = 'devolucao',
        nome_tabela = 'devolucao',
        prefixo = 'devolucao',
        data_processamento = DATA_PROCESSAMENTO,
        coluna_date='data_devolucao', 
        coluna_id='pedido_id',
    )

    ingest_bronze_to_silver(
        sistema = 'sistema_estoque',
        dominio = 'estoque',
        nome_tabela = 'estoque',
        prefixo = 'estoque',
        data_processamento = DATA_PROCESSAMENTO,
        coluna_date='data_referencia', 
        coluna_id='produto_id',
        append_only=True
    )

    ingest_bronze_to_silver(
        sistema = 'sistema_vendas',
        dominio = 'vendas',
        nome_tabela = 'vendas',
        prefixo = 'vendas',
        data_processamento = DATA_PROCESSAMENTO,
        coluna_date='data_venda', 
        coluna_id='pedido_id',
        append_only=True
    )

    ingest_bronze_to_silver(
        sistema = 'sistema_frete',
        dominio = 'frete',
        nome_tabela = 'frete',
        prefixo = 'frete',
        data_processamento = DATA_PROCESSAMENTO,
        coluna_date='data_envio', 
        coluna_id='pedido_id',
    )

    ingest_bronze_to_silver(
        sistema = 'sistema_tributos',
        dominio = 'tributos',
        nome_tabela = 'tributos',
        prefixo = 'tributos',
        data_processamento = DATA_PROCESSAMENTO,
        coluna_date='data_pagamento', 
        coluna_id='pedido_id',
        append_only=True
    )

"""
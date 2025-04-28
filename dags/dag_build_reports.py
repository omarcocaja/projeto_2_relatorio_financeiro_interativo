from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from delta import *
from faker import Faker
from pathlib import Path
import os
import pandas as pd
import pyspark
import pyspark.sql.functions as F
import random

BASE_DIR = '/home/omarcocaja/Área de trabalho/portfolio' # Caminho base para encontrar a pasta do datalake
DATALAKE_PATH = f"{BASE_DIR}/datalake"
BRONZE_PATH = f"{DATALAKE_PATH}/bronze"
SILVER_PATH = f"{DATALAKE_PATH}/silver"
GOLD_PATH = f"{DATALAKE_PATH}/gold"

builder = pyspark.sql.SparkSession.builder.appName("Projeto_2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

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
            
with DAG(
    dag_id="BUILD_REPORTS",
    start_date=datetime(2025, 4, 15),
    schedule="0 3 * * *",
    catchup=True
) as dag:
    
    def ingest_devolucao_from_bronze_to_silver(data_processamento: str) -> None:
        ingest_bronze_to_silver(
            sistema = 'sistema_devolucao',
            dominio = 'devolucao',
            nome_tabela = 'devolucao',
            prefixo = 'devolucao',
            data_processamento = data_processamento,
            coluna_date='data_devolucao', 
            coluna_id='pedido_id',
        )

    def ingest_estoque_from_bronze_to_silver(data_processamento: str) -> None:
        ingest_bronze_to_silver(
            sistema = 'sistema_estoque',
            dominio = 'estoque',
            nome_tabela = 'estoque',
            prefixo = 'estoque',
            data_processamento = data_processamento,
            coluna_date='data_referencia', 
            coluna_id='produto_id',
            append_only=True
        )
        
    def ingest_vendas_from_bronze_to_silver(data_processamento: str) -> None:
        ingest_bronze_to_silver(
            sistema = 'sistema_vendas',
            dominio = 'vendas',
            nome_tabela = 'vendas',
            prefixo = 'vendas',
            data_processamento = data_processamento,
            coluna_date='data_venda', 
            coluna_id='pedido_id',
            append_only=True
        )
    
    def ingest_frete_from_bronze_to_silver(data_processamento: str) -> None:
        ingest_bronze_to_silver(
            sistema = 'sistema_frete',
            dominio = 'frete',
            nome_tabela = 'frete',
            prefixo = 'frete',
            data_processamento = data_processamento,
            coluna_date='data_envio', 
            coluna_id='pedido_id',
        )

    def ingest_tributos_from_bronze_to_silver(data_processamento: str) -> None:
        ingest_bronze_to_silver(
            sistema = 'sistema_tributos',
            dominio = 'tributos',
            nome_tabela = 'tributos',
            prefixo = 'tributos',
            data_processamento = data_processamento,
            coluna_date='data_pagamento', 
            coluna_id='pedido_id',
            append_only=True
        )

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
    
    operator_devolucao_from_bronze_to_silver = PythonOperator(
        task_id = "INGEST_DEVOLUCAO_FROM_BRONZE_TO_SILVER",
        python_callable=ingest_devolucao_from_bronze_to_silver,
        provide_context=True,
        op_kwargs = {
            'data_processamento': "{{ ds }}"
        }
    )

    operator_estoque_from_bronze_to_silver = PythonOperator(
        task_id = "INGEST_ESTOQUE_FROM_BRONZE_TO_SILVER",
        python_callable=ingest_estoque_from_bronze_to_silver,
        provide_context=True,
        op_kwargs = {
            'data_processamento': "{{ ds }}"
        }
    )
    operator_vendas_from_bronze_to_silver = PythonOperator(
        task_id = "INGEST_VENDAS_FROM_BRONZE_TO_SILVER",
        python_callable=ingest_vendas_from_bronze_to_silver,
        provide_context=True,
        op_kwargs = {
            'data_processamento': "{{ ds }}"
        }
    )

    operator_frete_from_bronze_to_silver = PythonOperator(
        task_id = "INGEST_FRETE_FROM_BRONZE_TO_SILVER",
        python_callable=ingest_frete_from_bronze_to_silver,
        provide_context=True,
        op_kwargs = {
            'data_processamento': "{{ ds }}"
        }
    )

    operator_tributos_from_bronze_to_silver = PythonOperator(
        task_id = "INGEST_TRIBUTOS_FROM_BRONZE_TO_SILVER",
        python_callable=ingest_tributos_from_bronze_to_silver,
        provide_context=True,
        op_kwargs = {
            'data_processamento': "{{ ds }}"
        }
    )
    operator_create_gold_product_table = PythonOperator(
        task_id = "CREATE_GOLD_PRODUCT_TABLE",
        python_callable=create_gold_product_table,
        provide_context=True,
        op_kwargs = {
            'data_processamento': "{{ ds }}"
        }
    )
    operator_create_gold_freight_table = PythonOperator(
        task_id = "CREATE_GOLD_FREIGHT_TABLE",
        python_callable=create_gold_freight_table,
        provide_context=True,
        op_kwargs = {
            'data_processamento': "{{ ds }}"
        }
    )
    operator_create_gold_taxes_table = PythonOperator(
        task_id = "CREATE_GOLD_TAXES_TABLE",
        python_callable=create_gold_taxes_table,
        provide_context=True,
        op_kwargs = {
            'data_processamento': "{{ ds }}"
        }
    )

[
    operator_devolucao_from_bronze_to_silver, 
    operator_estoque_from_bronze_to_silver, 
    operator_vendas_from_bronze_to_silver
] >> operator_create_gold_product_table
operator_frete_from_bronze_to_silver >> operator_create_gold_freight_table
operator_tributos_from_bronze_to_silver >> operator_create_gold_taxes_table

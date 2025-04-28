# %%
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker
import pandas as pd
import random
import os

# %%
DATA_INICIO = (datetime.today() - timedelta(days=5))
DATA_FIM = datetime.today()
TOTAL_LINHAS_ARQUIVO = 1000

FILIAIS = [
    'SP01', 'SP02', 'SP03',
    'RJ01', 'RJ02', 'RJ03',
    'MG01', 'MG02', 'MG03',
    'BA01', 'BA02', 'BA03',
    'RS01', 'RS02', 'RS03',
    'PR01', 'PR02', 'PR03',
    'PE01', 'PE02', 'PE03',
    'MT01', 'MT02', 'MT03',
]

TIPOS_IMPOSTOS = ['ICMS', 'ISS', 'PIS', 'COFINS', 'IRPJ', 'CSLL', 'INSS', 'IPI']

MOTIVOS_DEVOLUCAO = [
    'Produto com defeito', 'Erro no pedido', 'Cliente desistiu', 'Atraso na entrega', 
    'Produto diferente do solicitado', 'Problema no pagamento'
]

BASE_DIR = str(Path.cwd().parent.parent)
DATALAKE_PATH = f"{BASE_DIR}/datalake"
BRONZE_PATH = f"{DATALAKE_PATH}/bronze"

fake = Faker('pt_BR')

# %%
def gerar_vendas_csv(inicio, fim, n):
    data_atual = inicio
    os.makedirs(f"{BRONZE_PATH}/sistema_vendas/vendas/", exist_ok=True)
    while data_atual <= fim:
        dados = []
        for _ in range(n):
            dados.append({
                'data_venda': data_atual.strftime('%Y-%m-%d'),
                'produto_id': random.randint(1, 50),
                'filial_id': random.choice(FILIAIS),
                'quantidade': random.randint(1, 100),
                'valor_unitario': round(random.uniform(10.0, 1500.0), 2)
            })
        file_name = f"vendas_{data_atual.strftime('%Y_%m_%d')}.csv"
        pd.DataFrame(dados).to_csv(f"{BRONZE_PATH}/sistema_vendas/vendas/{file_name}", index=False)
        data_atual += timedelta(days=1)

gerar_vendas_csv(DATA_INICIO, DATA_FIM, TOTAL_LINHAS_ARQUIVO)

# %%
def gerar_frete_csv(inicio, fim, n):
    data_atual = inicio
    os.makedirs(f"{BRONZE_PATH}/sistema_frete/frete/", exist_ok=True)
    while data_atual <= fim:
        dados = []
        for _ in range(n):
            dados.append({
                'pedido_id': fake.uuid4(),
                'valor_frete': round(random.uniform(15.0, 300.0), 2),
                'data_envio': data_atual.strftime('%Y-%m-%d'),
                'transportadora': random.choice(['Correios', 'JadLog', 'Loggi', 'Mandae']),
                'filial_id': random.choice(FILIAIS),
            })
        file_name = f"frete_{data_atual.strftime('%Y_%m_%d')}.csv"
        pd.DataFrame(dados).to_csv(f"{BRONZE_PATH}/sistema_frete/frete/{file_name}", index=False)
        data_atual += timedelta(days=1)

gerar_frete_csv(DATA_INICIO, DATA_FIM, TOTAL_LINHAS_ARQUIVO)

# %%
def gerar_tributos_csv(inicio, fim, n):
    data_atual = inicio
    os.makedirs(f"{BRONZE_PATH}/sistema_tributos/tributos/", exist_ok=True)
    while data_atual <= fim:
        dados = []
        for _ in range(n):
            dados.append({
                'data_pagamento': data_atual.strftime('%Y-%m-%d'),
                'tipo_imposto': random.choice(TIPOS_IMPOSTOS),
                'valor_pago': round(random.uniform(50.0, 5000.0), 2),
                'filial_id': random.choice(FILIAIS),
            })
        file_name = f"tributos_{data_atual.strftime('%Y_%m_%d')}.csv"
        pd.DataFrame(dados).to_csv(f"{BRONZE_PATH}/sistema_tributos/tributos/{file_name}", index=False)
        data_atual += timedelta(days=1)

gerar_tributos_csv(DATA_INICIO, DATA_FIM, TOTAL_LINHAS_ARQUIVO)

# %%
def gerar_estoque_csv(inicio, fim, n):
    data_atual = inicio
    os.makedirs(f"{BRONZE_PATH}/sistema_estoque/estoque/", exist_ok=True)
    while data_atual <= fim:
        dados = []
        for _ in range(n):
            dados.append({
                'produto_id': random.randint(1, 50),
                'filial_id': random.choice(FILIAIS),
                'quantidade_disponivel': random.randint(0, 500),
                'data_referencia': data_atual.strftime('%Y-%m-%d'),
            })
        file_name = f"estoque_{data_atual.strftime('%Y_%m_%d')}.csv"
        pd.DataFrame(dados).to_csv(f"{BRONZE_PATH}/sistema_estoque/estoque/{file_name}", index=False)
        data_atual += timedelta(days=1)

gerar_estoque_csv(DATA_INICIO, DATA_FIM, TOTAL_LINHAS_ARQUIVO)

# %%
def gerar_devolucao_csv(inicio, fim, n):
    data_atual = inicio
    os.makedirs(f"{BRONZE_PATH}/sistema_devolucao/devolucao/", exist_ok=True)
    while data_atual <= fim:
        dados = []
        for _ in range(n):
            dados.append({
                'pedido_id': fake.uuid4(),
                'produto_id': random.randint(1, 50),
                'quantidade': random.randint(1, 500),
                'motivo': random.choice(MOTIVOS_DEVOLUCAO),
                'data_devolucao': data_atual.strftime('%Y-%m-%d'),
                'filial_id': random.choice(FILIAIS),
            })
        file_name = f"devolucao_{data_atual.strftime('%Y_%m_%d')}.csv"
        pd.DataFrame(dados).to_csv(f"{BRONZE_PATH}/sistema_devolucao/devolucao/{file_name}", index=False)
        data_atual += timedelta(days=1)

gerar_devolucao_csv(DATA_INICIO, DATA_FIM, TOTAL_LINHAS_ARQUIVO)



# %%

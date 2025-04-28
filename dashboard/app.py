import streamlit as st
import pandas as pd
import pyspark 
from delta import configure_spark_with_delta_pip
import plotly.express as px 

BASE_DIR = '/home/omarcocaja/Área de trabalho/portfolio' # Caminho base para encontrar a pasta do datalake
DATALAKE_PATH = f"{BASE_DIR}/datalake"
GOLD_PATH = f"{DATALAKE_PATH}/gold"

builder = pyspark.sql.SparkSession.builder.appName("Projeto_2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

st.set_page_config(layout='wide')

st.markdown(
    """
    <style>
    .main .block-container{
        padding-top: 2rem;
        padding-botto: 2rem;
        padding-left: 1rem;
        padding-right: 1rem;
        max-width: 100%;
    }
    </style>
    """, unsafe_allow_html=True
)

@st.cache_data(ttl=300)
def carregar_tabelas_produtos():
    return spark.read.format('delta').load(f"{GOLD_PATH}/vendas/tb_gd_resumo_produtos_filiais").toPandas()

@st.cache_data(ttl=300)
def carregar_tabela_fretes():
    return spark.read.format('delta').load(f"{GOLD_PATH}/logistica/tb_gd_resumo_fretes_pagos").toPandas()

@st.cache_data(ttl=300)
def carregar_tabela_impostos():
    return spark.read.format('delta').load(f"{GOLD_PATH}/tributario/tb_gd_resumo_impostos_pagos").toPandas()

st.title("Dashboard Resumos Financeiros")

with st.expander("Filtros: ", expanded=True):
    col1, col2, col3 = st.columns(3)

    with col1:
        tabela_opcao = st.selectbox(
            'Selecione a tabela',
            ('Resumo Impostos', 'Resumo Fretes', 'Resumo Produtos')
        )

    if tabela_opcao == 'Resumo Impostos':
        df = carregar_tabela_impostos()
    elif tabela_opcao == 'Resumo Fretes':
        df = carregar_tabela_fretes()
    else:
        df = carregar_tabelas_produtos()

    with col2:
        filial_ids = sorted(df['FilialId'].unique())
        if filial_ids:
            filiais_selecionadas = st.multiselect(
                'Selecione as Filiais: ',
                options=filial_ids,
                default=[filial_ids[0]]
            )
        else:
            filiais_selecionadas = []
            st.warning("Não há filiais disponíveis.")
    
    with col3:
        if tabela_opcao == 'Resumo Produtos':
            produtos_ids = sorted(df['ProdutoId'].unique())
            produto_opcoes = ["*"] + list(produtos_ids)
            produto_selecionado = st.selectbox(
                'Selecione o Produto: ',
                produto_opcoes,
                index=0
            )
        else:
            produto_selecionado = None
            st.text("")

    datas_disponiveis = sorted(df['CodigoData'].unique())

    if len(datas_disponiveis) > 0:
        data_min = min(datas_disponiveis)
        data_max = max(datas_disponiveis)

        data_selecionada = st.select_slider(
            'Intervalo de datas: ',
            options= datas_disponiveis,
            value=(data_min, data_max)
        )
    
    else:
        data_selecionada = (None, None)
        st.warning('Não há datas disponíveis.')


if filiais_selecionadas:
    if tabela_opcao == 'Resumo Produtos':
        if produto_selecionado == "*":
            df_filtrado = df[
                (df['FilialId'].isin(filiais_selecionadas)) &
                (df['CodigoData'] >= data_selecionada[0]) &
                (df['CodigoData'] <= data_selecionada[1])
            ]
        else:
            df_filtrado = df[
                (df['ProdutoId'] == produto_selecionado) &
                (df['FilialId'].isin(filiais_selecionadas)) &
                (df['CodigoData'] >= data_selecionada[0]) &
                (df['CodigoData'] <= data_selecionada[1])
            ]
    else:
        df_filtrado = df[
            (df['FilialId'].isin(filiais_selecionadas)) &
            (df['CodigoData'] >= data_selecionada[0]) &
            (df['CodigoData'] <= data_selecionada[1])
        ]
    
    tab_tabela, tab_graficos = st.tabs(['Tabela Resumo' , 'Gráfico de Linhas'])

    with tab_tabela:
        st.dataframe(df_filtrado, use_container_width=True, height=500)
    
    with tab_graficos:
        if tabela_opcao == 'Resumo Impostos':
            df_agrupado = df_filtrado.groupby(['CodigoData', 'FilialId'])['TotalImpostoPago'].sum().reset_index()

            fig = px.line(
                df_agrupado,
                x='CodigoData',
                y='TotalImpostoPago',
                color='FilialId',
                title='Total de impostos por dia por filial',
                labels={
                    'TotalImpostoPago': 'Total de impostos',
                    'CodigoData': 'Data',
                    'FilialId': 'Filial'
                }
            )
        
        elif tabela_opcao == 'Resumo Fretes':
            df_agrupado = df_filtrado.groupby(['CodigoData', 'FilialId'])['TotalFretePago'].sum().reset_index()

            fig = px.line(
                df_agrupado,
                x='CodigoData',
                y='TotalFretePago',
                color='FilialId',
                title='Total de fretes por dia por filial',
                labels={
                    'TotalFretePago': 'Total de fretes',
                    'CodigoData': 'Data',
                    'FilialId': 'Filial'
                }
            )

        else:
            df_agrupado = df_filtrado.groupby(['CodigoData', 'FilialId'])['TotalQuantidadeVendida'].sum().reset_index()

            if produto_selecionado == "*":
                titulo = 'Quantidade total vendida por dia por filial - Todos os produtos'
            else:
                titulo = f"Quantidade total vendida por dia por filial - Produto {produto_selecionado}"
            
            fig = px.line(
                df_agrupado,
                x='CodigoData',
                y='TotalQuantidadeVendida',
                color='FilialId',
                title = titulo,
                labels={
                    'TotalQuantidadeVendida': 'Quantidade vendida',
                    'CodigoData': 'Data',
                    'FilialId': 'Filial'
                }
            )


        fig.update_layout(
            height = 600,
            legend_title_text='Filiais',
            xaxis_title='Data',
            hovermode= 'x unified'
        )

        st.plotly_chart(fig, use_container_width=True)

else:
    st.warning('Por favor, selecione pelo menos uma filial para visualizar os dados.')
# Relatório Financeiro Interativo com Airflow, Spark e Streamlit

Este projeto simula uma arquitetura de dados moderna com ingestão em batch e construção de relatórios financeiros interativos. O pipeline completo percorre as camadas **bronze**, **silver** e **gold**, com orquestração via **Apache Airflow**, processamento com **PySpark** e visualização com **Streamlit**.

---

## 🗂 Estrutura do Projeto

```
projeto_2_relatorio_financeiro_interativo/
│
├── dags/
│   └── dag_build_reports.py           # DAG responsável por orquestrar a construção das camadas do lake
│
├── scripts/
│   ├── mock_generator.py              # Gera os dados simulados e particionados por data
│   ├── ingest_bronze_to_silver.py     # Faz ingestão da bronze para silver
│   ├── create_gold_product_table.py   # Gera a tabela de produtos na camada gold
│   ├── create_gold_taxes_table.py     # Gera a tabela de impostos na camada gold
│   └── create_gold_freight_table.py   # Gera a tabela de fretes na camada gold
│
├── dashboard/
│   └── app.py                         # Aplicação Streamlit com os relatórios financeiros interativos
│
├── imagens/
│   └── *.png                          # Capturas de tela da aplicação
│
├── requirements.txt                   # Bibliotecas necessárias
└── README.md                          # Este arquivo
```

> ⚠️ É necessário criar a pasta `datalake/` na raiz com as subpastas `bronze/`, `silver/` e `gold/`. Os scripts já assumem essa estrutura como destino dos dados.

---

## ⚙️ Como Executar

### 1. Instale as dependências
```bash
pip install -r requirements.txt
```

### 2. Gere os dados simulados
```bash
python scripts/mock_generator.py
```

### 3. Inicie o Airflow standalone
```bash
airflow standalone
```

### 4. Ative a DAG `BUILD_REPORTS`
Acesse o painel do Airflow em [http://localhost:8080](http://localhost:8080) e ative a DAG `BUILD_REPORTS`.

### 5. Rode o app Streamlit
```bash
streamlit run dashboard/app.py
```

---

## 📦 Tecnologias Utilizadas

- **Apache Airflow**
- **Apache Spark (PySpark)**
- **Streamlit**
- **Delta Lake**
- **Python 3.10+**

---

## 📄 Licença

Este projeto está licenciado sob a licença **MIT** e é livre para estudo e customização.

---

## 📬 Contato

- [LinkedIn](https://www.linkedin.com/in/marco-caja)  
- [Instagram](https://www.instagram.com/omarcocaja)

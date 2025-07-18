## Global Emissions Pipeline (Em desenvolvimento)

**Climate Data Pipeline** é uma estrutura modular e escalável que está sendo desenvolvida para orquestrar a extração, transformação e carregamento (ETL) de dados climáticos e socioeconômicos globais. O projeto integra dados da **ClimateTrace API** e **World Bank API**, realiza o processamento com **Spark**, transforma os dados com **dbt**, e armazena os resultados no **BigQuery** e **Google Cloud Storage**.

*Está sendo desenvolvido como parte do Data Engineering Zoomcamp, com alterações adicionais para aprendizado/testes, ambientes de produção e/ou possível uso institucional.*

### Requisitos

- Python 3.9+
- Apache Airflow 2.6+
- Spark 3.x
- GCP (GCS e BigQuery)
- dbt
- Bibliotecas: requests, pandas, tqdm, pyarrow, google-cloud-*

### Pipeline de Orquestração (Airflow)

| DAG ID                                     | Descrição                                                                  |
| ------------------------------------------ | -------------------------------------------------------------------------- |
| `climate_data_pipeline_multi_year`         | Extrai dados da World Bank e ClimateTrace (por ano) e armazena em GCS      |
| `climate_data_spark_processing`            | Processa dados extraídos com Spark e gera tabelas no BigQuery              |
| `climate_data_spark_historical_processing` | Pipeline histórico para múltiplos anos usando Spark                        |
| `climate_data_dbt_transformations`         | Executa `dbt run`, `dbt test` e `dbt docs generate` sobre dados combinados |
| `climate_data_dbt_transformations_2025`    | Versão dedicada ao ano de 2025                                             |
| `spark_env_setup.py`                       | Configura ambiente Spark (JAVA\_HOME, SPARK\_HOME, etc.)                   |

### Scripts de Extração e Processamento

1. climate_data_extractor.py
Script CLI que extrai dados brutos das APIs:
```
$ python climate_data_extractor.py world_bank 2018 --to_year 2023
$ python climate_data_extractor.py climate_trace 2020
```

2. climate_trace_extractor.py
Extrai e salva dados de emissões anuais da API ClimateTrace com robustez e suporte a múltiplos anos.
```
$ python climate_trace_extractor.py 2015 --to_year 2022
```

3. climate_wb_indicator_extractor.py
Extrai indicadores econômicos da World Bank para todos os países.
```
$ python climate_wb_indicator_extractor.py 2010 --end_year 2020
```

4. climate_econ_processor.py
Transforma arquivos CSV em Parquet e combina dados socioeconômicos e climáticos. Funciona com dados salvos em GCS.

Funções principais:
- `process_world_bank_csv()`
- `process_climate_csv()`
- `combine_by_year()`

### Integração com GCP

- Google Cloud Storage (GCS): Armazenamento de arquivos CSV e Parquet
- BigQuery: Criação de tabelas externas para análises
- dbt: Modelagem analítica e validação de qualidade dos dados

### Configurações via Airflow Variables

Configure via Admin → Variables no Airflow UI:

| Variável           | Descrição                             | Exemplo                  |
| ------------------ | ------------------------------------- | ------------------------ |
| `gcs_raw_bucket`   | Bucket para arquivos brutos           | `zoomcamp-climate-trace` |
| `processing_year`  | Ano padrão de processamento           | `2025`                   |
| `extraction_years` | Lista de anos para extração           | `"[2020,2021,2022]"`     |
| `java_home`        | Caminho para o Java                   | `/opt/java`              |
| `spark_home`       | Caminho para o Spark                  | `/opt/spark`             |
| `airflow_python`   | Caminho opcional do Python no Airflow | `/usr/bin/python3`       |

### Funcionalidades 

- Retry e Timeout automáticos com requests.Session e Retry
- Sensores externos para dependências entre DAGs (ExternalTaskSensorAsync)
- Geração automática de artefatos dbt (dbt docs)
- Particionamento e clustering no BigQuery
- Tipagem estática com from __future__ import annotations
- Caminhos com pathlib

### Alguns Tipos de Exemplos de Indicadores World Bank

| Código           | Descrição                          |
| ---------------- | ---------------------------------- |
| `SP.POP.TOTL`    | População total                    |
| `NY.GDP.PCAP.CD` | PIB per capita                     |
| `SI.POV.GAPS`    | Gap de pobreza (US\$2.15)          |
| `SP.DYN.LE00.IN` | Expectativa de vida ao nascer      |
| `SE.SEC.ENRR`    | Matrícula no ensino secundário (%) |
| `SI.POV.GINI`    | Índice de Gini                     |
| `SL.UEM.TOTL.ZS` | Desemprego (% força de trabalho)   |

#### Contribuição
Contribuições são bem-vindas.

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # BCB_00 · Setup
-- MAGIC Catálogo `economia`, schemas por camada, volumes de landing e tabelas de configuração das fontes do Banco Central:
-- MAGIC
-- MAGIC - **SGS**: séries temporais (Selic, CDI, dólar, IPCA, IGP-M, desocupação).
-- MAGIC - **Olinda**: taxas de juros cobradas por instituição financeira e expectativas de mercado do relatório Focus.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS economia;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS economia.config COMMENT 'Parâmetros do pipeline';
CREATE SCHEMA IF NOT EXISTS economia.landing COMMENT 'Respostas brutas da API em arquivos JSON';
CREATE SCHEMA IF NOT EXISTS economia.bronze COMMENT 'Dados como chegaram da API, com metadados de carga';
CREATE SCHEMA IF NOT EXISTS economia.silver COMMENT 'Dados tipados, validados e deduplicados';
CREATE SCHEMA IF NOT EXISTS economia.gold COMMENT 'Modelo estrela para consumo analítico';
CREATE SCHEMA IF NOT EXISTS economia.monitoramento COMMENT 'Resultados de qualidade de dados e volumetria por execução';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS economia.landing.bcb_sgs COMMENT 'Arquivos JSON retornados pela API SGS do BCB';
CREATE VOLUME IF NOT EXISTS economia.landing.bcb_olinda COMMENT 'Arquivos JSON Lines compactados retornados pelas APIs Olinda do BCB';

-- COMMAND ----------

CREATE OR REPLACE TABLE economia.config.series_sgs (
  codigo_serie INT NOT NULL COMMENT 'Código da série no SGS',
  nome STRING NOT NULL,
  nome_curto STRING NOT NULL COMMENT 'Identificador usado nas transformações',
  unidade STRING NOT NULL,
  periodicidade STRING NOT NULL,
  categoria STRING NOT NULL,
  data_inicio DATE NOT NULL COMMENT 'Primeira data carregada na carga inicial',
  ativa BOOLEAN NOT NULL,
  CONSTRAINT pk_series_sgs PRIMARY KEY (codigo_serie)
)
COMMENT 'Séries do SGS consumidas pelo pipeline';

INSERT INTO economia.config.series_sgs VALUES
  (432,   'Meta da taxa Selic definida pelo Copom', 'selic_meta',  '% a.a.',  'diaria', 'juros',     DATE'2015-01-01', true),
  (12,    'Taxa CDI',                               'cdi',         '% a.d.',  'diaria', 'juros',     DATE'2015-01-01', true),
  (1,     'Dólar comercial (venda) - PTAX',         'dolar',       'R$/US$',  'diaria', 'cambio',    DATE'2015-01-01', true),
  (433,   'IPCA - variação mensal',                 'ipca',        '% a.m.',  'mensal', 'inflacao',  DATE'2015-01-01', true),
  (13522, 'IPCA - acumulado em 12 meses',           'ipca_12m',    '%',       'mensal', 'inflacao',  DATE'2015-01-01', true),
  (189,   'IGP-M - variação mensal',                'igpm',        '% a.m.',  'mensal', 'inflacao',  DATE'2015-01-01', true),
  (24369, 'Taxa de desocupação - PNAD Contínua',    'desocupacao', '%',       'mensal', 'atividade', DATE'2015-01-01', true);

-- COMMAND ----------

CREATE OR REPLACE TABLE economia.config.meta_inflacao (
  ano INT NOT NULL,
  meta DECIMAL(5,2) NOT NULL COMMENT 'Meta de inflação definida pelo CMN (%)',
  tolerancia DECIMAL(5,2) NOT NULL COMMENT 'Intervalo de tolerância (p.p.)',
  CONSTRAINT pk_meta_inflacao PRIMARY KEY (ano)
)
COMMENT 'Metas de inflação do CMN por ano';

INSERT INTO economia.config.meta_inflacao VALUES
  (2015, 4.50, 2.00), (2016, 4.50, 2.00), (2017, 4.50, 1.50), (2018, 4.50, 1.50),
  (2019, 4.25, 1.50), (2020, 4.00, 1.50), (2021, 3.75, 1.50), (2022, 3.50, 1.50),
  (2023, 3.25, 1.50), (2024, 3.00, 1.50), (2025, 3.00, 1.50), (2026, 3.00, 1.50),
  (2027, 3.00, 1.50);

-- COMMAND ----------

CREATE OR REPLACE TABLE economia.config.fontes_olinda (
  fonte STRING NOT NULL COMMENT 'Identificador da fonte, usado em diretórios e checkpoints',
  descricao STRING NOT NULL,
  servico_url STRING NOT NULL COMMENT 'Endpoint OData do recurso',
  campo_data STRING NOT NULL COMMENT 'Campo usado para paginar a extração por mês',
  data_inicio DATE NOT NULL COMMENT 'Primeiro mês extraído na carga inicial',
  tabela_bronze STRING NOT NULL,
  ativa BOOLEAN NOT NULL,
  CONSTRAINT pk_fontes_olinda PRIMARY KEY (fonte)
)
COMMENT 'Recursos das APIs Olinda consumidos pelo pipeline';

INSERT INTO economia.config.fontes_olinda VALUES
  ('taxas_juros_diaria',
   'Taxas de juros por instituição financeira e modalidade, médias de 5 dias úteis',
   'https://olinda.bcb.gov.br/olinda/servico/taxaJuros/versao/v2/odata/TaxasJurosDiariaPorInicioPeriodo',
   'InicioPeriodo', DATE'2012-01-01', 'olinda_taxas_juros', true),
  ('expectativas_anuais',
   'Expectativas de mercado anuais do relatório Focus',
   'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais',
   'Data', DATE'2014-01-01', 'olinda_expectativas_anuais', true);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.monitoramento.qualidade_resultados (
  execucao_id STRING NOT NULL COMMENT 'Run id do job ou manual',
  executado_em TIMESTAMP NOT NULL,
  regra STRING NOT NULL,
  tabela STRING NOT NULL,
  severidade STRING NOT NULL COMMENT 'erro interrompe o job; aviso só registra',
  violacoes BIGINT NOT NULL,
  aprovado BOOLEAN NOT NULL
)
COMMENT 'Resultado de cada regra de qualidade por execução';

CREATE TABLE IF NOT EXISTS economia.monitoramento.volumetria (
  execucao_id STRING NOT NULL,
  executado_em TIMESTAMP NOT NULL,
  tabela STRING NOT NULL COMMENT 'esquema.tabela',
  linhas BIGINT NOT NULL
)
COMMENT 'Contagem de linhas de cada tabela por execução';

-- COMMAND ----------

SELECT 'sgs' AS api, CAST(codigo_serie AS STRING) AS fonte, nome AS descricao, data_inicio
FROM economia.config.series_sgs
UNION ALL
SELECT 'olinda', fonte, descricao, data_inicio
FROM economia.config.fontes_olinda
ORDER BY api, fonte;

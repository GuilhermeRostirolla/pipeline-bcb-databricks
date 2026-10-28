-- Databricks notebook source
-- MAGIC %md
-- MAGIC # BCB_03 · Silver
-- MAGIC Tipa, valida e deduplica os dados da bronze das três fontes.
-- MAGIC
-- MAGIC - Registros inválidos vão para tabelas de quarentena com o motivo da rejeição.
-- MAGIC - Registros válidos entram por `MERGE`: a versão mais recente de cada chave prevalece, o que absorve revisões do BCB.
-- MAGIC - As fontes Olinda são processadas de forma incremental: só entram registros da bronze carregados após a última execução.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SGS · séries temporais

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.silver.sgs_valores (
  codigo_serie INT NOT NULL,
  data DATE NOT NULL,
  valor DECIMAL(18,6) NOT NULL,
  _arquivo_origem STRING NOT NULL,
  _data_carga TIMESTAMP NOT NULL,
  _atualizado_em TIMESTAMP NOT NULL,
  CONSTRAINT pk_silver_sgs_valores PRIMARY KEY (codigo_serie, data)
)
COMMENT 'Valores das séries SGS tipados e deduplicados. Uma linha por série e data.';

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sgs_tipado AS
SELECT
  b.codigo_serie,
  try_to_date(b.data, 'dd/MM/yyyy') AS data,
  try_cast(b.valor AS DECIMAL(18,6)) AS valor,
  b.data AS data_original,
  b.valor AS valor_original,
  b._rescued_data,
  b._arquivo_origem,
  b._data_carga,
  s.codigo_serie IS NOT NULL AS serie_cadastrada
FROM economia.bronze.sgs_valores AS b
LEFT JOIN economia.config.series_sgs AS s
  ON s.codigo_serie = b.codigo_serie;

-- COMMAND ----------

CREATE OR REPLACE TABLE economia.silver.sgs_quarentena
COMMENT 'Registros da bronze rejeitados pelas regras de qualidade da silver'
AS
SELECT
  codigo_serie,
  data_original,
  valor_original,
  _rescued_data,
  _arquivo_origem,
  _data_carga,
  CASE
    WHEN NOT serie_cadastrada THEN 'serie_nao_cadastrada'
    WHEN data IS NULL THEN 'data_invalida'
    WHEN valor IS NULL THEN 'valor_invalido'
    WHEN data > current_date() THEN 'data_futura'
    ELSE 'campos_inesperados'
  END AS motivo
FROM sgs_tipado
WHERE NOT serie_cadastrada
   OR data IS NULL
   OR valor IS NULL
   OR data > current_date()
   OR _rescued_data IS NOT NULL;

-- COMMAND ----------

MERGE INTO economia.silver.sgs_valores AS destino
USING (
  SELECT codigo_serie, data, valor, _arquivo_origem, _data_carga
  FROM sgs_tipado
  WHERE serie_cadastrada
    AND data IS NOT NULL
    AND valor IS NOT NULL
    AND data <= current_date()
    AND _rescued_data IS NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY codigo_serie, data ORDER BY _arquivo_origem DESC) = 1
) AS origem
ON destino.codigo_serie = origem.codigo_serie
AND destino.data = origem.data
WHEN MATCHED AND destino.valor <> origem.valor THEN UPDATE SET
  destino.valor = origem.valor,
  destino._arquivo_origem = origem._arquivo_origem,
  destino._data_carga = origem._data_carga,
  destino._atualizado_em = current_timestamp()
WHEN NOT MATCHED THEN INSERT (codigo_serie, data, valor, _arquivo_origem, _data_carga, _atualizado_em)
  VALUES (origem.codigo_serie, origem.data, origem.valor, origem._arquivo_origem, origem._data_carga, current_timestamp());

-- COMMAND ----------

WITH bronze AS (
  SELECT codigo_serie, COUNT(*) AS linhas_bronze, COUNT(DISTINCT data_original) AS datas_distintas_bronze
  FROM sgs_tipado
  GROUP BY codigo_serie
),
silver AS (
  SELECT codigo_serie, COUNT(*) AS linhas_silver, MIN(data) AS primeira_data, MAX(data) AS ultima_data
  FROM economia.silver.sgs_valores
  GROUP BY codigo_serie
),
quarentena AS (
  SELECT codigo_serie, COUNT(*) AS linhas_quarentena
  FROM economia.silver.sgs_quarentena
  GROUP BY codigo_serie
)
SELECT
  s.codigo_serie,
  s.nome_curto,
  b.linhas_bronze,
  b.datas_distintas_bronze,
  sv.linhas_silver,
  COALESCE(q.linhas_quarentena, 0) AS linhas_quarentena,
  b.linhas_bronze - b.datas_distintas_bronze AS duplicadas_por_reprocessamento,
  sv.primeira_data,
  sv.ultima_data
FROM economia.config.series_sgs AS s
LEFT JOIN bronze AS b ON b.codigo_serie = s.codigo_serie
LEFT JOIN silver AS sv ON sv.codigo_serie = s.codigo_serie
LEFT JOIN quarentena AS q ON q.codigo_serie = s.codigo_serie
ORDER BY s.codigo_serie;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Olinda · taxas de juros por instituição financeira

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.silver.taxas_juros (
  inicio_periodo DATE NOT NULL COMMENT 'Primeiro dia útil da janela de 5 dias úteis',
  fim_periodo DATE NOT NULL,
  segmento STRING NOT NULL,
  modalidade STRING NOT NULL,
  cnpj8 STRING NOT NULL COMMENT 'Raiz do CNPJ da instituição',
  instituicao STRING NOT NULL,
  posicao INT NOT NULL COMMENT 'Ranking da instituição na modalidade, da menor para a maior taxa',
  taxa_mes DECIMAL(10,4) NOT NULL COMMENT 'Taxa média ao mês (%)',
  taxa_ano DECIMAL(10,4) NOT NULL COMMENT 'Taxa média ao ano (%)',
  _arquivo_origem STRING NOT NULL,
  _data_carga TIMESTAMP NOT NULL,
  _atualizado_em TIMESTAMP NOT NULL,
  CONSTRAINT pk_silver_taxas_juros PRIMARY KEY (inicio_periodo, segmento, modalidade, cnpj8)
)
COMMENT 'Taxas de juros por instituição, segmento, modalidade e período. Uma linha por chave.';

CREATE TABLE IF NOT EXISTS economia.silver.taxas_juros_quarentena (
  registro STRING NOT NULL COMMENT 'Registro original em JSON',
  motivo STRING NOT NULL,
  _arquivo_origem STRING NOT NULL,
  _data_carga TIMESTAMP NOT NULL,
  _quarentenado_em TIMESTAMP NOT NULL
)
COMMENT 'Registros de taxas de juros rejeitados pelas regras da silver';

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW taxas_juros_novas AS
SELECT
  try_to_date(InicioPeriodo) AS inicio_periodo,
  try_to_date(FimPeriodo) AS fim_periodo,
  nullif(trim(Segmento), '') AS segmento,
  nullif(trim(Modalidade), '') AS modalidade,
  lpad(nullif(trim(cnpj8), ''), 8, '0') AS cnpj8,
  nullif(trim(InstituicaoFinanceira), '') AS instituicao,
  try_cast(Posicao AS INT) AS posicao,
  try_cast(TaxaJurosAoMes AS DECIMAL(10,4)) AS taxa_mes,
  try_cast(TaxaJurosAoAno AS DECIMAL(10,4)) AS taxa_ano,
  to_json(struct(InicioPeriodo, FimPeriodo, Segmento, Modalidade, cnpj8, InstituicaoFinanceira, Posicao, TaxaJurosAoMes, TaxaJurosAoAno)) AS registro,
  _rescued_data,
  _arquivo_origem,
  _data_carga
FROM economia.bronze.olinda_taxas_juros
WHERE _data_carga > (
  SELECT coalesce(max(_data_carga), TIMESTAMP'1900-01-01')
  FROM (
    SELECT _data_carga FROM economia.silver.taxas_juros
    UNION ALL
    SELECT _data_carga FROM economia.silver.taxas_juros_quarentena
  )
);

-- COMMAND ----------

INSERT INTO economia.silver.taxas_juros_quarentena
SELECT
  registro,
  CASE
    WHEN inicio_periodo IS NULL OR fim_periodo IS NULL THEN 'data_invalida'
    WHEN segmento IS NULL OR modalidade IS NULL OR cnpj8 IS NULL OR instituicao IS NULL THEN 'chave_incompleta'
    WHEN taxa_mes IS NULL OR taxa_ano IS NULL OR posicao IS NULL THEN 'valor_invalido'
    WHEN fim_periodo < inicio_periodo THEN 'periodo_invertido'
    ELSE 'campos_inesperados'
  END,
  _arquivo_origem,
  _data_carga,
  current_timestamp()
FROM taxas_juros_novas
WHERE inicio_periodo IS NULL OR fim_periodo IS NULL
   OR segmento IS NULL OR modalidade IS NULL OR cnpj8 IS NULL OR instituicao IS NULL
   OR taxa_mes IS NULL OR taxa_ano IS NULL OR posicao IS NULL
   OR fim_periodo < inicio_periodo
   OR _rescued_data IS NOT NULL;

-- COMMAND ----------

MERGE INTO economia.silver.taxas_juros AS destino
USING (
  SELECT inicio_periodo, fim_periodo, segmento, modalidade, cnpj8, instituicao, posicao, taxa_mes, taxa_ano, _arquivo_origem, _data_carga
  FROM taxas_juros_novas
  WHERE inicio_periodo IS NOT NULL AND fim_periodo IS NOT NULL
    AND segmento IS NOT NULL AND modalidade IS NOT NULL AND cnpj8 IS NOT NULL AND instituicao IS NOT NULL
    AND taxa_mes IS NOT NULL AND taxa_ano IS NOT NULL AND posicao IS NOT NULL
    AND fim_periodo >= inicio_periodo
    AND _rescued_data IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY inicio_periodo, segmento, modalidade, cnpj8
    ORDER BY _arquivo_origem DESC, posicao
  ) = 1
) AS origem
ON destino.inicio_periodo = origem.inicio_periodo
AND destino.segmento = origem.segmento
AND destino.modalidade = origem.modalidade
AND destino.cnpj8 = origem.cnpj8
WHEN MATCHED AND (
  destino.taxa_mes <> origem.taxa_mes
  OR destino.taxa_ano <> origem.taxa_ano
  OR destino.posicao <> origem.posicao
  OR destino.instituicao <> origem.instituicao
  OR destino.fim_periodo <> origem.fim_periodo
) THEN UPDATE SET
  destino.fim_periodo = origem.fim_periodo,
  destino.instituicao = origem.instituicao,
  destino.posicao = origem.posicao,
  destino.taxa_mes = origem.taxa_mes,
  destino.taxa_ano = origem.taxa_ano,
  destino._arquivo_origem = origem._arquivo_origem,
  destino._data_carga = origem._data_carga,
  destino._atualizado_em = current_timestamp()
WHEN MATCHED THEN UPDATE SET
  destino._data_carga = origem._data_carga
WHEN NOT MATCHED THEN INSERT (
  inicio_periodo, fim_periodo, segmento, modalidade, cnpj8, instituicao, posicao, taxa_mes, taxa_ano, _arquivo_origem, _data_carga, _atualizado_em
) VALUES (
  origem.inicio_periodo, origem.fim_periodo, origem.segmento, origem.modalidade, origem.cnpj8, origem.instituicao,
  origem.posicao, origem.taxa_mes, origem.taxa_ano, origem._arquivo_origem, origem._data_carga, current_timestamp()
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Olinda · expectativas de mercado (Focus)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.silver.expectativas_anuais (
  data DATE NOT NULL COMMENT 'Data de divulgação da estatística',
  indicador STRING NOT NULL,
  indicador_detalhe STRING NOT NULL COMMENT 'Abertura do indicador; Total quando não há abertura',
  ano_referencia INT NOT NULL COMMENT 'Ano a que a projeção se refere',
  base_calculo INT NOT NULL COMMENT '0 = respostas dos últimos 30 dias; 1 = últimos 5 dias úteis',
  media DECIMAL(18,4),
  mediana DECIMAL(18,4) NOT NULL,
  desvio_padrao DECIMAL(18,4),
  minimo DECIMAL(18,4),
  maximo DECIMAL(18,4),
  respondentes INT,
  _arquivo_origem STRING NOT NULL,
  _data_carga TIMESTAMP NOT NULL,
  _atualizado_em TIMESTAMP NOT NULL,
  CONSTRAINT pk_silver_expectativas_anuais PRIMARY KEY (data, indicador, indicador_detalhe, ano_referencia, base_calculo)
)
COMMENT 'Estatísticas diárias das projeções anuais do Focus. Uma linha por chave.';

CREATE TABLE IF NOT EXISTS economia.silver.expectativas_anuais_quarentena (
  registro STRING NOT NULL COMMENT 'Registro original em JSON',
  motivo STRING NOT NULL,
  _arquivo_origem STRING NOT NULL,
  _data_carga TIMESTAMP NOT NULL,
  _quarentenado_em TIMESTAMP NOT NULL
)
COMMENT 'Registros do Focus rejeitados pelas regras da silver';

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW expectativas_novas AS
SELECT
  try_to_date(Data) AS data,
  nullif(trim(Indicador), '') AS indicador,
  coalesce(nullif(trim(IndicadorDetalhe), ''), 'Total') AS indicador_detalhe,
  try_cast(DataReferencia AS INT) AS ano_referencia,
  try_cast(baseCalculo AS INT) AS base_calculo,
  try_cast(Media AS DECIMAL(18,4)) AS media,
  try_cast(Mediana AS DECIMAL(18,4)) AS mediana,
  try_cast(DesvioPadrao AS DECIMAL(18,4)) AS desvio_padrao,
  try_cast(Minimo AS DECIMAL(18,4)) AS minimo,
  try_cast(Maximo AS DECIMAL(18,4)) AS maximo,
  try_cast(numeroRespondentes AS INT) AS respondentes,
  to_json(struct(Indicador, IndicadorDetalhe, Data, DataReferencia, Media, Mediana, DesvioPadrao, Minimo, Maximo, numeroRespondentes, baseCalculo)) AS registro,
  _rescued_data,
  _arquivo_origem,
  _data_carga
FROM economia.bronze.olinda_expectativas_anuais
WHERE _data_carga > (
  SELECT coalesce(max(_data_carga), TIMESTAMP'1900-01-01')
  FROM (
    SELECT _data_carga FROM economia.silver.expectativas_anuais
    UNION ALL
    SELECT _data_carga FROM economia.silver.expectativas_anuais_quarentena
  )
);

-- COMMAND ----------

INSERT INTO economia.silver.expectativas_anuais_quarentena
SELECT
  registro,
  CASE
    WHEN data IS NULL THEN 'data_invalida'
    WHEN indicador IS NULL OR ano_referencia IS NULL OR base_calculo IS NULL THEN 'chave_incompleta'
    WHEN mediana IS NULL THEN 'valor_invalido'
    ELSE 'campos_inesperados'
  END,
  _arquivo_origem,
  _data_carga,
  current_timestamp()
FROM expectativas_novas
WHERE data IS NULL OR indicador IS NULL OR ano_referencia IS NULL OR base_calculo IS NULL
   OR mediana IS NULL
   OR _rescued_data IS NOT NULL;

-- COMMAND ----------

MERGE INTO economia.silver.expectativas_anuais AS destino
USING (
  SELECT data, indicador, indicador_detalhe, ano_referencia, base_calculo, media, mediana, desvio_padrao, minimo, maximo, respondentes, _arquivo_origem, _data_carga
  FROM expectativas_novas
  WHERE data IS NOT NULL AND indicador IS NOT NULL AND ano_referencia IS NOT NULL AND base_calculo IS NOT NULL
    AND mediana IS NOT NULL
    AND _rescued_data IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY data, indicador, indicador_detalhe, ano_referencia, base_calculo
    ORDER BY _arquivo_origem DESC
  ) = 1
) AS origem
ON destino.data = origem.data
AND destino.indicador = origem.indicador
AND destino.indicador_detalhe = origem.indicador_detalhe
AND destino.ano_referencia = origem.ano_referencia
AND destino.base_calculo = origem.base_calculo
WHEN MATCHED AND (
  destino.mediana <> origem.mediana
  OR NOT (destino.media <=> origem.media)
  OR NOT (destino.respondentes <=> origem.respondentes)
) THEN UPDATE SET
  destino.media = origem.media,
  destino.mediana = origem.mediana,
  destino.desvio_padrao = origem.desvio_padrao,
  destino.minimo = origem.minimo,
  destino.maximo = origem.maximo,
  destino.respondentes = origem.respondentes,
  destino._arquivo_origem = origem._arquivo_origem,
  destino._data_carga = origem._data_carga,
  destino._atualizado_em = current_timestamp()
WHEN MATCHED THEN UPDATE SET
  destino._data_carga = origem._data_carga
WHEN NOT MATCHED THEN INSERT (
  data, indicador, indicador_detalhe, ano_referencia, base_calculo, media, mediana, desvio_padrao, minimo, maximo, respondentes, _arquivo_origem, _data_carga, _atualizado_em
) VALUES (
  origem.data, origem.indicador, origem.indicador_detalhe, origem.ano_referencia, origem.base_calculo, origem.media, origem.mediana,
  origem.desvio_padrao, origem.minimo, origem.maximo, origem.respondentes, origem._arquivo_origem, origem._data_carga, current_timestamp()
);

-- COMMAND ----------

SELECT 'sgs_valores' AS tabela, COUNT(*) AS linhas, (SELECT COUNT(*) FROM economia.silver.sgs_quarentena) AS quarentena FROM economia.silver.sgs_valores
UNION ALL
SELECT 'taxas_juros', COUNT(*), (SELECT COUNT(*) FROM economia.silver.taxas_juros_quarentena) FROM economia.silver.taxas_juros
UNION ALL
SELECT 'expectativas_anuais', COUNT(*), (SELECT COUNT(*) FROM economia.silver.expectativas_anuais_quarentena) FROM economia.silver.expectativas_anuais;

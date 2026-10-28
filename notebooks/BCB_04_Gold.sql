-- Databricks notebook source
-- MAGIC %md
-- MAGIC # BCB_04 · Gold
-- MAGIC Modelo estrela para consumo analítico.
-- MAGIC
-- MAGIC | Tabela | Grão | Uso |
-- MAGIC |---|---|---|
-- MAGIC | `dim_calendario` | dia | eixo de tempo comum aos fatos |
-- MAGIC | `dim_serie` | série SGS | atributos de cada indicador |
-- MAGIC | `fato_indicador` | série × dia | valores originais de todas as séries |
-- MAGIC | `fato_macro_mensal` | mês | indicadores mensais, acumulados em 12 meses, juro real e meta de inflação |
-- MAGIC | `dim_instituicao` | instituição financeira | nome mais recente por raiz de CNPJ |
-- MAGIC | `dim_modalidade` | segmento × modalidade | produto de crédito e tipo de encargo |
-- MAGIC | `fato_taxa_juros` | período × instituição × modalidade | taxa cobrada e posição no ranking |
-- MAGIC | `dim_indicador_focus` | indicador × detalhe | indicadores projetados no Focus |
-- MAGIC | `fato_expectativa_anual` | data × indicador × ano de referência × base | estatísticas das projeções do mercado |
-- MAGIC
-- MAGIC As tabelas são criadas uma vez com chaves e comentários; cada execução recarrega o conteúdo com `INSERT OVERWRITE`, preservando o esquema e as constraints.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.gold.dim_calendario (
  data DATE NOT NULL,
  ano INT NOT NULL,
  mes INT NOT NULL,
  mes_nome STRING NOT NULL,
  trimestre INT NOT NULL,
  ano_mes STRING NOT NULL COMMENT 'Formato yyyy-MM',
  primeiro_dia_mes DATE NOT NULL,
  dia_semana INT NOT NULL COMMENT '1 = domingo, 7 = sábado',
  nome_dia STRING NOT NULL,
  dia_util BOOLEAN NOT NULL COMMENT 'Segunda a sexta, sem considerar feriados',
  CONSTRAINT pk_dim_calendario PRIMARY KEY (data)
)
COMMENT 'Calendário diário de 2012 a 2027';

INSERT OVERWRITE economia.gold.dim_calendario
SELECT
  d AS data,
  year(d) AS ano,
  month(d) AS mes,
  element_at(array('jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'), month(d)) AS mes_nome,
  quarter(d) AS trimestre,
  date_format(d, 'yyyy-MM') AS ano_mes,
  trunc(d, 'MM') AS primeiro_dia_mes,
  dayofweek(d) AS dia_semana,
  element_at(array('dom', 'seg', 'ter', 'qua', 'qui', 'sex', 'sáb'), dayofweek(d)) AS nome_dia,
  dayofweek(d) NOT IN (1, 7) AS dia_util
FROM (SELECT explode(sequence(DATE'2012-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS d);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.gold.dim_serie (
  codigo_serie INT NOT NULL COMMENT 'Código da série no SGS',
  nome STRING NOT NULL,
  nome_curto STRING NOT NULL,
  unidade STRING NOT NULL,
  periodicidade STRING NOT NULL,
  categoria STRING NOT NULL,
  CONSTRAINT pk_dim_serie PRIMARY KEY (codigo_serie)
)
COMMENT 'Séries do SGS disponíveis no modelo';

INSERT OVERWRITE economia.gold.dim_serie
SELECT codigo_serie, nome, nome_curto, unidade, periodicidade, categoria
FROM economia.config.series_sgs;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.gold.fato_indicador (
  codigo_serie INT NOT NULL,
  data DATE NOT NULL,
  valor DECIMAL(18,6) NOT NULL COMMENT 'Valor publicado pelo BCB, na unidade da série',
  CONSTRAINT pk_fato_indicador PRIMARY KEY (codigo_serie, data),
  CONSTRAINT fk_fato_indicador_serie FOREIGN KEY (codigo_serie) REFERENCES economia.gold.dim_serie (codigo_serie),
  CONSTRAINT fk_fato_indicador_calendario FOREIGN KEY (data) REFERENCES economia.gold.dim_calendario (data)
)
COMMENT 'Valores de todas as séries SGS, uma linha por série e data';

INSERT OVERWRITE economia.gold.fato_indicador
SELECT codigo_serie, data, valor
FROM economia.silver.sgs_valores;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.gold.fato_macro_mensal (
  mes DATE NOT NULL COMMENT 'Primeiro dia do mês',
  selic_meta DECIMAL(10,4) COMMENT 'Meta Selic vigente no último dia do mês (% a.a.)',
  cdi_mes DECIMAL(10,4) COMMENT 'CDI acumulado no mês (%)',
  cdi_12m DECIMAL(10,4) COMMENT 'CDI acumulado em 12 meses (%)',
  ipca_mes DECIMAL(10,4) COMMENT 'IPCA do mês (%)',
  ipca_12m DECIMAL(10,4) COMMENT 'IPCA acumulado em 12 meses (%)',
  igpm_mes DECIMAL(10,4) COMMENT 'IGP-M do mês (%)',
  igpm_12m DECIMAL(10,4) COMMENT 'IGP-M acumulado em 12 meses (%)',
  dolar_fim_mes DECIMAL(10,4) COMMENT 'PTAX venda do último dia útil do mês (R$)',
  dolar_medio_mes DECIMAL(10,4) COMMENT 'Média da PTAX venda no mês (R$)',
  dolar_var_mes DECIMAL(10,4) COMMENT 'Variação do dólar de fim de mês contra o mês anterior (%)',
  desocupacao DECIMAL(10,4) COMMENT 'Taxa de desocupação da PNAD Contínua (%)',
  juro_real_12m DECIMAL(10,4) COMMENT 'Selic meta descontada do IPCA 12 meses (%)',
  cdi_real_12m DECIMAL(10,4) COMMENT 'CDI 12 meses descontado do IPCA 12 meses (%)',
  meta_inflacao DECIMAL(10,4) COMMENT 'Meta de inflação do CMN para o ano (%)',
  piso_meta DECIMAL(10,4),
  teto_meta DECIMAL(10,4),
  ipca_acima_teto BOOLEAN COMMENT 'IPCA 12 meses acima do teto da meta',
  mes_completo BOOLEAN NOT NULL COMMENT 'Falso para o mês corrente, ainda em formação',
  CONSTRAINT pk_fato_macro_mensal PRIMARY KEY (mes),
  CONSTRAINT fk_fato_macro_mensal_calendario FOREIGN KEY (mes) REFERENCES economia.gold.dim_calendario (data)
)
COMMENT 'Painel macroeconômico mensal: juros, inflação, câmbio e emprego';

INSERT OVERWRITE economia.gold.fato_macro_mensal
WITH valores AS (
  SELECT s.nome_curto, f.data, trunc(f.data, 'MM') AS mes, CAST(f.valor AS DOUBLE) AS valor
  FROM economia.silver.sgs_valores AS f
  JOIN economia.config.series_sgs AS s
    ON s.codigo_serie = f.codigo_serie
),
meses AS (
  SELECT DISTINCT primeiro_dia_mes AS mes
  FROM economia.gold.dim_calendario
  WHERE data BETWEEN (SELECT MIN(data) FROM valores) AND current_date()
),
agregado AS (
  SELECT
    mes,
    max_by(valor, data) FILTER (WHERE nome_curto = 'selic_meta') AS selic_meta,
    (exp(sum(ln(1 + valor / 100)) FILTER (WHERE nome_curto = 'cdi')) - 1) * 100 AS cdi_mes,
    max(valor) FILTER (WHERE nome_curto = 'ipca') AS ipca_mes,
    max(valor) FILTER (WHERE nome_curto = 'ipca_12m') AS ipca_12m,
    max(valor) FILTER (WHERE nome_curto = 'igpm') AS igpm_mes,
    max_by(valor, data) FILTER (WHERE nome_curto = 'dolar') AS dolar_fim_mes,
    avg(valor) FILTER (WHERE nome_curto = 'dolar') AS dolar_medio_mes,
    max(valor) FILTER (WHERE nome_curto = 'desocupacao') AS desocupacao
  FROM valores
  GROUP BY mes
),
acumulado AS (
  SELECT
    m.mes,
    a.selic_meta,
    a.cdi_mes,
    CASE WHEN count(a.cdi_mes) OVER w12 = 12 THEN (exp(sum(ln(1 + a.cdi_mes / 100)) OVER w12) - 1) * 100 END AS cdi_12m,
    a.ipca_mes,
    a.ipca_12m,
    a.igpm_mes,
    CASE WHEN count(a.igpm_mes) OVER w12 = 12 THEN (exp(sum(ln(1 + a.igpm_mes / 100)) OVER w12) - 1) * 100 END AS igpm_12m,
    a.dolar_fim_mes,
    a.dolar_medio_mes,
    (a.dolar_fim_mes / lag(a.dolar_fim_mes) OVER (ORDER BY m.mes) - 1) * 100 AS dolar_var_mes,
    a.desocupacao
  FROM meses AS m
  LEFT JOIN agregado AS a
    ON a.mes = m.mes
  WINDOW w12 AS (ORDER BY m.mes ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
)
SELECT
  c.mes,
  CAST(c.selic_meta AS DECIMAL(10,4)),
  CAST(c.cdi_mes AS DECIMAL(10,4)),
  CAST(c.cdi_12m AS DECIMAL(10,4)),
  CAST(c.ipca_mes AS DECIMAL(10,4)),
  CAST(c.ipca_12m AS DECIMAL(10,4)),
  CAST(c.igpm_mes AS DECIMAL(10,4)),
  CAST(c.igpm_12m AS DECIMAL(10,4)),
  CAST(c.dolar_fim_mes AS DECIMAL(10,4)),
  CAST(c.dolar_medio_mes AS DECIMAL(10,4)),
  CAST(c.dolar_var_mes AS DECIMAL(10,4)),
  CAST(c.desocupacao AS DECIMAL(10,4)),
  CAST(((1 + c.selic_meta / 100) / (1 + c.ipca_12m / 100) - 1) * 100 AS DECIMAL(10,4)),
  CAST(((1 + c.cdi_12m / 100) / (1 + c.ipca_12m / 100) - 1) * 100 AS DECIMAL(10,4)),
  CAST(mi.meta AS DECIMAL(10,4)),
  CAST(mi.meta - mi.tolerancia AS DECIMAL(10,4)),
  CAST(mi.meta + mi.tolerancia AS DECIMAL(10,4)),
  c.ipca_12m > mi.meta + mi.tolerancia,
  c.mes < trunc(current_date(), 'MM')
FROM acumulado AS c
LEFT JOIN economia.config.meta_inflacao AS mi
  ON mi.ano = year(c.mes);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.gold.dim_instituicao (
  cnpj8 STRING NOT NULL COMMENT 'Raiz do CNPJ',
  instituicao STRING NOT NULL COMMENT 'Nome mais recente informado ao BCB',
  CONSTRAINT pk_dim_instituicao PRIMARY KEY (cnpj8)
)
COMMENT 'Instituições financeiras presentes nas taxas de juros';

INSERT OVERWRITE economia.gold.dim_instituicao
SELECT cnpj8, max_by(instituicao, inicio_periodo)
FROM economia.silver.taxas_juros
GROUP BY cnpj8;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.gold.dim_modalidade (
  modalidade_id BIGINT NOT NULL COMMENT 'Hash estável de segmento e modalidade',
  segmento STRING NOT NULL,
  modalidade STRING NOT NULL,
  produto STRING NOT NULL COMMENT 'Linha de crédito, sem o tipo de encargo',
  encargo STRING NOT NULL COMMENT 'Prefixado, pós-fixado e variações',
  CONSTRAINT pk_dim_modalidade PRIMARY KEY (modalidade_id)
)
COMMENT 'Modalidades de crédito por segmento';

INSERT OVERWRITE economia.gold.dim_modalidade
SELECT
  xxhash64(segmento, modalidade),
  segmento,
  modalidade,
  CASE WHEN modalidade LIKE '% - %' THEN left(modalidade, length(modalidade) - length(substring_index(modalidade, ' - ', -1)) - 3) ELSE modalidade END,
  CASE WHEN modalidade LIKE '% - %' THEN substring_index(modalidade, ' - ', -1) ELSE 'Não informado' END
FROM (SELECT DISTINCT segmento, modalidade FROM economia.silver.taxas_juros);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.gold.fato_taxa_juros (
  data DATE NOT NULL COMMENT 'Início da janela de 5 dias úteis',
  fim_periodo DATE NOT NULL,
  cnpj8 STRING NOT NULL,
  modalidade_id BIGINT NOT NULL,
  posicao INT NOT NULL COMMENT 'Ranking da instituição na modalidade, da menor para a maior taxa',
  taxa_mes DECIMAL(10,4) NOT NULL COMMENT 'Taxa média ao mês (%)',
  taxa_ano DECIMAL(10,4) NOT NULL COMMENT 'Taxa média ao ano (%)',
  CONSTRAINT pk_fato_taxa_juros PRIMARY KEY (data, cnpj8, modalidade_id),
  CONSTRAINT fk_fato_taxa_juros_calendario FOREIGN KEY (data) REFERENCES economia.gold.dim_calendario (data),
  CONSTRAINT fk_fato_taxa_juros_instituicao FOREIGN KEY (cnpj8) REFERENCES economia.gold.dim_instituicao (cnpj8),
  CONSTRAINT fk_fato_taxa_juros_modalidade FOREIGN KEY (modalidade_id) REFERENCES economia.gold.dim_modalidade (modalidade_id)
)
COMMENT 'Taxas de juros cobradas por instituição e modalidade';

INSERT OVERWRITE economia.gold.fato_taxa_juros
SELECT inicio_periodo, fim_periodo, cnpj8, xxhash64(segmento, modalidade), posicao, taxa_mes, taxa_ano
FROM economia.silver.taxas_juros;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.gold.dim_indicador_focus (
  indicador_id BIGINT NOT NULL COMMENT 'Hash estável de indicador e detalhe',
  indicador STRING NOT NULL,
  indicador_detalhe STRING NOT NULL,
  CONSTRAINT pk_dim_indicador_focus PRIMARY KEY (indicador_id)
)
COMMENT 'Indicadores projetados no relatório Focus';

INSERT OVERWRITE economia.gold.dim_indicador_focus
SELECT xxhash64(indicador, indicador_detalhe), indicador, indicador_detalhe
FROM (SELECT DISTINCT indicador, indicador_detalhe FROM economia.silver.expectativas_anuais);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS economia.gold.fato_expectativa_anual (
  data DATE NOT NULL COMMENT 'Data de divulgação',
  indicador_id BIGINT NOT NULL,
  ano_referencia INT NOT NULL,
  base_calculo INT NOT NULL COMMENT '0 = últimos 30 dias; 1 = últimos 5 dias úteis',
  mediana DECIMAL(18,4) NOT NULL,
  media DECIMAL(18,4),
  desvio_padrao DECIMAL(18,4),
  minimo DECIMAL(18,4),
  maximo DECIMAL(18,4),
  respondentes INT,
  CONSTRAINT pk_fato_expectativa_anual PRIMARY KEY (data, indicador_id, ano_referencia, base_calculo),
  CONSTRAINT fk_fato_expectativa_anual_calendario FOREIGN KEY (data) REFERENCES economia.gold.dim_calendario (data),
  CONSTRAINT fk_fato_expectativa_anual_indicador FOREIGN KEY (indicador_id) REFERENCES economia.gold.dim_indicador_focus (indicador_id)
)
COMMENT 'Projeções anuais do mercado divulgadas no Focus';

INSERT OVERWRITE economia.gold.fato_expectativa_anual
SELECT data, xxhash64(indicador, indicador_detalhe), ano_referencia, base_calculo, mediana, media, desvio_padrao, minimo, maximo, respondentes
FROM economia.silver.expectativas_anuais;

-- COMMAND ----------

SELECT 'dim_calendario' AS tabela, COUNT(*) AS linhas FROM economia.gold.dim_calendario
UNION ALL SELECT 'dim_serie', COUNT(*) FROM economia.gold.dim_serie
UNION ALL SELECT 'fato_indicador', COUNT(*) FROM economia.gold.fato_indicador
UNION ALL SELECT 'fato_macro_mensal', COUNT(*) FROM economia.gold.fato_macro_mensal
UNION ALL SELECT 'dim_instituicao', COUNT(*) FROM economia.gold.dim_instituicao
UNION ALL SELECT 'dim_modalidade', COUNT(*) FROM economia.gold.dim_modalidade
UNION ALL SELECT 'fato_taxa_juros', COUNT(*) FROM economia.gold.fato_taxa_juros
UNION ALL SELECT 'dim_indicador_focus', COUNT(*) FROM economia.gold.dim_indicador_focus
UNION ALL SELECT 'fato_expectativa_anual', COUNT(*) FROM economia.gold.fato_expectativa_anual;

-- COMMAND ----------

SELECT *
FROM economia.gold.fato_macro_mensal
ORDER BY mes DESC
LIMIT 13;

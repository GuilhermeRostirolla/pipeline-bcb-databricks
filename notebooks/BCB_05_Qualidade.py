# Databricks notebook source
# MAGIC %md
# MAGIC # BCB_05 · Qualidade e volumetria
# MAGIC Executa as regras de qualidade sobre silver e gold e registra o resultado de cada execução em `economia.monitoramento`.
# MAGIC
# MAGIC - Cada regra conta violações. Severidade `erro` interrompe o job; `aviso` só fica registrado.
# MAGIC - A volumetria guarda a contagem de linhas de cada tabela por execução, para acompanhar o crescimento e detectar cargas vazias.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F

dbutils.widgets.text("execucao_id", "manual")
EXECUCAO_ID = dbutils.widgets.get("execucao_id")
EXECUTADO_EM = datetime.now(timezone.utc)
CATALOGO = "economia"

# COMMAND ----------

REGRAS = [
    ("pk_fato_indicador", "gold.fato_indicador", "erro",
     "SELECT COUNT(*) FROM (SELECT codigo_serie, data FROM {c}.gold.fato_indicador GROUP BY ALL HAVING COUNT(*) > 1)"),
    ("pk_fato_taxa_juros", "gold.fato_taxa_juros", "erro",
     "SELECT COUNT(*) FROM (SELECT data, cnpj8, modalidade_id FROM {c}.gold.fato_taxa_juros GROUP BY ALL HAVING COUNT(*) > 1)"),
    ("pk_fato_expectativa_anual", "gold.fato_expectativa_anual", "erro",
     "SELECT COUNT(*) FROM (SELECT data, indicador_id, ano_referencia, base_calculo FROM {c}.gold.fato_expectativa_anual GROUP BY ALL HAVING COUNT(*) > 1)"),
    ("fk_taxa_juros_instituicao", "gold.fato_taxa_juros", "erro",
     "SELECT COUNT(*) FROM {c}.gold.fato_taxa_juros f LEFT ANTI JOIN {c}.gold.dim_instituicao d USING (cnpj8)"),
    ("fk_taxa_juros_modalidade", "gold.fato_taxa_juros", "erro",
     "SELECT COUNT(*) FROM {c}.gold.fato_taxa_juros f LEFT ANTI JOIN {c}.gold.dim_modalidade d USING (modalidade_id)"),
    ("fk_taxa_juros_calendario", "gold.fato_taxa_juros", "erro",
     "SELECT COUNT(*) FROM {c}.gold.fato_taxa_juros f LEFT ANTI JOIN {c}.gold.dim_calendario d USING (data)"),
    ("fk_expectativa_indicador", "gold.fato_expectativa_anual", "erro",
     "SELECT COUNT(*) FROM {c}.gold.fato_expectativa_anual f LEFT ANTI JOIN {c}.gold.dim_indicador_focus d USING (indicador_id)"),
    ("fk_expectativa_calendario", "gold.fato_expectativa_anual", "erro",
     "SELECT COUNT(*) FROM {c}.gold.fato_expectativa_anual f LEFT ANTI JOIN {c}.gold.dim_calendario d USING (data)"),
    ("fk_indicador_calendario", "gold.fato_indicador", "erro",
     "SELECT COUNT(*) FROM {c}.gold.fato_indicador f LEFT ANTI JOIN {c}.gold.dim_calendario d USING (data)"),
    ("reconciliacao_taxa_juros", "gold.fato_taxa_juros", "erro",
     "SELECT abs((SELECT COUNT(*) FROM {c}.silver.taxas_juros) - (SELECT COUNT(*) FROM {c}.gold.fato_taxa_juros))"),
    ("reconciliacao_expectativas", "gold.fato_expectativa_anual", "erro",
     "SELECT abs((SELECT COUNT(*) FROM {c}.silver.expectativas_anuais) - (SELECT COUNT(*) FROM {c}.gold.fato_expectativa_anual))"),
    ("taxa_ano_fora_da_faixa", "silver.taxas_juros", "aviso",
     "SELECT COUNT(*) FROM {c}.silver.taxas_juros WHERE taxa_ano < 0 OR taxa_ano > 1000"),
    ("taxa_mes_maior_que_ano", "silver.taxas_juros", "aviso",
     "SELECT COUNT(*) FROM {c}.silver.taxas_juros WHERE taxa_mes > taxa_ano AND taxa_ano > 0"),
    ("focus_mediana_fora_do_intervalo", "silver.expectativas_anuais", "aviso",
     "SELECT COUNT(*) FROM {c}.silver.expectativas_anuais WHERE mediana < minimo OR mediana > maximo"),
    ("focus_sem_respondentes", "silver.expectativas_anuais", "aviso",
     "SELECT COUNT(*) FROM {c}.silver.expectativas_anuais WHERE respondentes IS NULL OR respondentes <= 0"),
    ("dolar_nao_positivo", "silver.sgs_valores", "erro",
     "SELECT COUNT(*) FROM {c}.silver.sgs_valores WHERE codigo_serie = 1 AND valor <= 0"),
    ("ipca_mensal_atipico", "silver.sgs_valores", "aviso",
     "SELECT COUNT(*) FROM {c}.silver.sgs_valores WHERE codigo_serie = 433 AND abs(valor) > 5"),
    ("sgs_desatualizado", "silver.sgs_valores", "aviso",
     "SELECT CASE WHEN MAX(data) < date_sub(current_date(), 7) THEN 1 ELSE 0 END FROM {c}.silver.sgs_valores WHERE codigo_serie = 432"),
    ("taxas_juros_desatualizadas", "silver.taxas_juros", "aviso",
     "SELECT CASE WHEN MAX(inicio_periodo) < date_sub(current_date(), 45) THEN 1 ELSE 0 END FROM {c}.silver.taxas_juros"),
    ("focus_desatualizado", "silver.expectativas_anuais", "aviso",
     "SELECT CASE WHEN MAX(data) < date_sub(current_date(), 10) THEN 1 ELSE 0 END FROM {c}.silver.expectativas_anuais"),
]

resultados = [
    (EXECUCAO_ID, EXECUTADO_EM, regra, tabela, severidade, int(spark.sql(sql.format(c=CATALOGO)).first()[0]))
    for regra, tabela, severidade, sql in REGRAS
]

df_resultados = spark.createDataFrame(
    resultados,
    "execucao_id STRING, executado_em TIMESTAMP, regra STRING, tabela STRING, severidade STRING, violacoes LONG",
).withColumn("aprovado", F.col("violacoes") == 0)

df_resultados.write.mode("append").saveAsTable(f"{CATALOGO}.monitoramento.qualidade_resultados")
display(df_resultados.orderBy(F.col("aprovado"), F.col("severidade"), F.col("regra")))

# COMMAND ----------

tabelas = [
    f"{esquema}.{linha.tableName}"
    for esquema in ("bronze", "silver", "gold")
    for linha in spark.sql(f"SHOW TABLES IN {CATALOGO}.{esquema}").collect()
    if not linha.isTemporary
]
volumetria = [(EXECUCAO_ID, EXECUTADO_EM, tabela, spark.table(f"{CATALOGO}.{tabela}").count()) for tabela in tabelas]

df_volumetria = spark.createDataFrame(
    volumetria,
    "execucao_id STRING, executado_em TIMESTAMP, tabela STRING, linhas LONG",
)
df_volumetria.write.mode("append").saveAsTable(f"{CATALOGO}.monitoramento.volumetria")
display(df_volumetria.orderBy("tabela"))

# COMMAND ----------

falhas = [r for r in resultados if r[4] == "erro" and r[5] > 0]
if falhas:
    raise RuntimeError("Regras de qualidade com severidade erro falharam: " + ", ".join(f"{r[2]} ({r[5]})" for r in falhas))

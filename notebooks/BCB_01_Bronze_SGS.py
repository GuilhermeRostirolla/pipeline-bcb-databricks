# Databricks notebook source
# MAGIC %md
# MAGIC # BCB_01 · Bronze SGS
# MAGIC Extrai as séries ativas da API SGS do Banco Central, grava as respostas brutas no volume de landing e carrega na bronze com Auto Loader.
# MAGIC
# MAGIC - Carga inicial a partir de `data_inicio` de cada série; cargas seguintes reprocessam os últimos `DIAS_REPROCESSAMENTO` dias para capturar revisões.
# MAGIC - Séries diárias são paginadas em janelas de `JANELA_DIAS`, pois a API recusa intervalos acima de 10 anos.

# COMMAND ----------

import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from pyspark.sql import functions as F
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CATALOGO = "economia"
VOLUME = f"/Volumes/{CATALOGO}/landing/bcb_sgs"
DIRETORIO_RAW = f"{VOLUME}/raw"
CHECKPOINT = f"{VOLUME}/_checkpoints/sgs_valores"
TABELA_CONFIG = f"{CATALOGO}.config.series_sgs"
TABELA_BRONZE = f"{CATALOGO}.bronze.sgs_valores"
URL_SERIE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
JANELA_DIAS = 5 * 365
DIAS_REPROCESSAMENTO = 30
TIMEOUT_SEGUNDOS = 60
TENTATIVAS_CONTEUDO = 5
ESPERA_SEGUNDOS = 30

# COMMAND ----------

def criar_sessao() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    sessao = requests.Session()
    sessao.mount("https://", HTTPAdapter(max_retries=retry))
    sessao.headers.update({"Accept": "application/json"})
    return sessao


def janelas(inicio: date, fim: date, tamanho_dias: int):
    atual = inicio
    while atual <= fim:
        limite = min(atual + timedelta(days=tamanho_dias - 1), fim)
        yield atual, limite
        atual = limite + timedelta(days=1)


def buscar_serie(sessao: requests.Session, codigo: int, inicio: date, fim: date) -> list:
    parametros = {
        "formato": "json",
        "dataInicial": inicio.strftime("%d/%m/%Y"),
        "dataFinal": fim.strftime("%d/%m/%Y"),
    }
    for tentativa in range(1, TENTATIVAS_CONTEUDO + 1):
        resposta = sessao.get(URL_SERIE.format(codigo=codigo), params=parametros, timeout=TIMEOUT_SEGUNDOS)
        if resposta.status_code == 404:
            return []
        resposta.raise_for_status()
        if not resposta.text.strip():
            return []
        try:
            return resposta.json()
        except ValueError:
            if tentativa == TENTATIVAS_CONTEUDO:
                raise RuntimeError(
                    f"Série {codigo}: resposta não JSON após {TENTATIVAS_CONTEUDO} tentativas: {resposta.text[:200]}"
                )
            time.sleep(ESPERA_SEGUNDOS * tentativa)


def ultima_data_por_serie() -> dict:
    if not spark.catalog.tableExists(TABELA_BRONZE):
        return {}
    linhas = (
        spark.table(TABELA_BRONZE)
        .groupBy("codigo_serie")
        .agg(F.max(F.expr("try_to_date(data, 'dd/MM/yyyy')")).alias("ultima_data"))
        .collect()
    )
    return {linha.codigo_serie: linha.ultima_data for linha in linhas if linha.ultima_data}

# COMMAND ----------

series = spark.table(TABELA_CONFIG).where("ativa").orderBy("codigo_serie").collect()
ultimas_datas = ultima_data_por_serie()
hoje = date.today()
carimbo = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
sessao = criar_sessao()
resumo = []

for serie in series:
    ultima = ultimas_datas.get(serie.codigo_serie)
    inicio = max(serie.data_inicio, ultima - timedelta(days=DIAS_REPROCESSAMENTO)) if ultima else serie.data_inicio
    registros = 0
    arquivos = 0
    for inicio_janela, fim_janela in janelas(inicio, hoje, JANELA_DIAS):
        dados = buscar_serie(sessao, serie.codigo_serie, inicio_janela, fim_janela)
        if not dados:
            continue
        caminho = f"{DIRETORIO_RAW}/serie={serie.codigo_serie}/{carimbo}_{inicio_janela:%Y%m%d}_{fim_janela:%Y%m%d}.json"
        arquivo = Path(caminho)
        arquivo.parent.mkdir(parents=True, exist_ok=True)
        arquivo.write_text(json.dumps(dados), encoding="utf-8")
        registros += len(dados)
        arquivos += 1
    resumo.append((serie.codigo_serie, serie.nome_curto, inicio, arquivos, registros))

display(
    spark.createDataFrame(
        resumo,
        "codigo_serie INT, serie STRING, extraido_desde DATE, arquivos INT, registros INT",
    )
)

# COMMAND ----------

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema("data STRING, valor STRING")
    .load(DIRETORIO_RAW)
    .select(
        F.regexp_extract(F.col("_metadata.file_path"), r"serie=(\d+)", 1).cast("int").alias("codigo_serie"),
        "data",
        "valor",
        "_rescued_data",
        F.col("_metadata.file_path").alias("_arquivo_origem"),
        F.current_timestamp().alias("_data_carga"),
    )
    .writeStream.option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TABELA_BRONZE)
    .awaitTermination()
)

# COMMAND ----------

display(
    spark.table(TABELA_BRONZE)
    .groupBy("codigo_serie")
    .agg(
        F.count("*").alias("linhas"),
        F.countDistinct("_arquivo_origem").alias("arquivos"),
        F.min(F.expr("try_to_date(data, 'dd/MM/yyyy')")).alias("primeira_data"),
        F.max(F.expr("try_to_date(data, 'dd/MM/yyyy')")).alias("ultima_data"),
    )
    .orderBy("codigo_serie")
)

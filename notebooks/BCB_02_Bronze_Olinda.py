# Databricks notebook source
# MAGIC %md
# MAGIC # BCB_02 · Bronze Olinda
# MAGIC Extrai os recursos ativos das APIs Olinda do Banco Central (taxas de juros por instituição financeira e expectativas do Focus), grava as respostas como JSON Lines compactado no volume de landing e carrega na bronze com Auto Loader.
# MAGIC
# MAGIC - A extração é paginada por mês no campo de data de cada recurso e roda em paralelo.
# MAGIC - A query OData é codificada com `%20` nos espaços: a API rejeita `+` com HTTP 400.
# MAGIC - Carga inicial a partir de `data_inicio`; cargas seguintes reprocessam os últimos `MESES_REPROCESSAMENTO` meses para capturar revisões.
# MAGIC - O esquema é inferido como texto e versionado em `_schemas`; a tipagem acontece na silver.

# COMMAND ----------

import gzip
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from functools import reduce
from pathlib import Path
from urllib.parse import urlencode, quote

import requests
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CATALOGO = "economia"
VOLUME = f"/Volumes/{CATALOGO}/landing/bcb_olinda"
TABELA_CONFIG = f"{CATALOGO}.config.fontes_olinda"
MESES_REPROCESSAMENTO = 1
LIMITE_REGISTROS = 200_000
PARALELISMO = 4
TIMEOUT_SEGUNDOS = 120
TENTATIVAS_CONTEUDO = 5
ESPERA_SEGUNDOS = 30

# COMMAND ----------

_contexto = threading.local()


def sessao() -> requests.Session:
    if not hasattr(_contexto, "sessao"):
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        nova = requests.Session()
        nova.mount("https://", HTTPAdapter(max_retries=retry))
        nova.headers.update({"Accept": "application/json"})
        _contexto.sessao = nova
    return _contexto.sessao


def meses(inicio: date, fim: date):
    atual = inicio.replace(day=1)
    while atual <= fim:
        yield atual
        atual += relativedelta(months=1)


def buscar_mes(url: str, campo_data: str, mes: date) -> list:
    proximo = mes + relativedelta(months=1)
    parametros = urlencode(
        {
            "$format": "json",
            "$top": str(LIMITE_REGISTROS),
            "$filter": f"{campo_data} ge '{mes:%Y-%m-%d}' and {campo_data} lt '{proximo:%Y-%m-%d}'",
        },
        quote_via=quote,
    )
    for tentativa in range(1, TENTATIVAS_CONTEUDO + 1):
        resposta = sessao().get(f"{url}?{parametros}", timeout=TIMEOUT_SEGUNDOS)
        resposta.raise_for_status()
        try:
            registros = resposta.json()["value"]
        except (ValueError, KeyError):
            if tentativa == TENTATIVAS_CONTEUDO:
                raise RuntimeError(
                    f"{url} {mes:%Y-%m}: resposta inválida após {TENTATIVAS_CONTEUDO} tentativas: {resposta.text[:200]}"
                )
            time.sleep(ESPERA_SEGUNDOS * tentativa)
            continue
        if len(registros) >= LIMITE_REGISTROS:
            raise RuntimeError(f"{url} {mes:%Y-%m}: limite de {LIMITE_REGISTROS} registros atingido, reduza a janela")
        return registros


def ultimo_mes_carregado(tabela: str, campo_data: str) -> date | None:
    if not spark.catalog.tableExists(tabela):
        return None
    valor = spark.table(tabela).agg(F.max(campo_data)).first()[0]
    return date.fromisoformat(valor[:10]).replace(day=1) if valor else None

# COMMAND ----------

fontes = spark.table(TABELA_CONFIG).where("ativa").orderBy("fonte").collect()
hoje = date.today()
carimbo = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
resumo = []

for fonte in fontes:
    tabela = f"{CATALOGO}.bronze.{fonte.tabela_bronze}"
    ultimo = ultimo_mes_carregado(tabela, fonte.campo_data)
    inicio = (
        max(fonte.data_inicio, ultimo - relativedelta(months=MESES_REPROCESSAMENTO))
        if ultimo
        else fonte.data_inicio
    )
    diretorio = Path(f"{VOLUME}/raw/{fonte.fonte}")
    diretorio.mkdir(parents=True, exist_ok=True)

    def extrair(mes: date, fonte=fonte, diretorio=diretorio) -> int:
        registros = buscar_mes(fonte.servico_url, fonte.campo_data, mes)
        if not registros:
            return 0
        conteudo = "\n".join(json.dumps(registro, ensure_ascii=False) for registro in registros)
        (diretorio / f"{carimbo}_{mes:%Y%m}.jsonl.gz").write_bytes(gzip.compress(conteudo.encode("utf-8")))
        return len(registros)

    with ThreadPoolExecutor(max_workers=PARALELISMO) as executor:
        contagens = list(executor.map(extrair, meses(inicio, hoje)))

    resumo.append((fonte.fonte, inicio, sum(1 for c in contagens if c), sum(contagens)))

display(
    spark.createDataFrame(
        resumo,
        "fonte STRING, extraido_desde DATE, arquivos INT, registros LONG",
    )
)

# COMMAND ----------

for fonte in fontes:
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{VOLUME}/_schemas/{fonte.fonte}")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{VOLUME}/raw/{fonte.fonte}")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_arquivo_origem"),
            F.current_timestamp().alias("_data_carga"),
        )
        .writeStream.option("checkpointLocation", f"{VOLUME}/_checkpoints/{fonte.fonte}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CATALOGO}.bronze.{fonte.tabela_bronze}")
        .awaitTermination()
    )

# COMMAND ----------

resumos = [
    spark.table(f"{CATALOGO}.bronze.{fonte.tabela_bronze}").agg(
        F.lit(fonte.fonte).alias("fonte"),
        F.count("*").alias("linhas"),
        F.countDistinct("_arquivo_origem").alias("arquivos"),
        F.min(fonte.campo_data).alias("primeira_data"),
        F.max(fonte.campo_data).alias("ultima_data"),
    )
    for fonte in fontes
]
display(reduce(DataFrame.unionByName, resumos))

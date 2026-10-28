# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # BCB_06 · Job agendado
# MAGIC Cria ou atualiza, como código, o job `bcb_pipeline_diario` em dias úteis às 18h (horário de Brasília), em computação serverless:
# MAGIC
# MAGIC `bronze_sgs` e `bronze_olinda` em paralelo → `silver` → `gold` → `qualidade`
# MAGIC
# MAGIC Reexecutar este notebook não duplica o job: se ele já existir, a configuração é sobrescrita.

# COMMAND ----------

from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

NOME_JOB = "bcb_pipeline_diario"
NOME_JOB_ANTERIOR = "bcb_sgs_pipeline_diario"
CRON = "0 0 18 ? * MON-FRI"
FUSO = "America/Sao_Paulo"

w = WorkspaceClient()
caminho_atual = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
pasta = caminho_atual.rsplit("/", 1)[0]


def tarefa(chave: str, notebook: str, depende_de: tuple[str, ...] = (), parametros: dict | None = None) -> jobs.Task:
    return jobs.Task(
        task_key=chave,
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{pasta}/{notebook}",
            source=jobs.Source.WORKSPACE,
            base_parameters=parametros,
        ),
        depends_on=[jobs.TaskDependency(task_key=d) for d in depende_de] or None,
        max_retries=2,
        min_retry_interval_millis=300_000,
        timeout_seconds=3600,
    )


parametros = dict(
    name=NOME_JOB,
    tasks=[
        tarefa("bronze_sgs", "BCB_01_Bronze_SGS"),
        tarefa("bronze_olinda", "BCB_02_Bronze_Olinda"),
        tarefa("silver", "BCB_03_Silver", ("bronze_sgs", "bronze_olinda")),
        tarefa("gold", "BCB_04_Gold", ("silver",)),
        tarefa("qualidade", "BCB_05_Qualidade", ("gold",), {"execucao_id": "{{job.run_id}}"}),
    ],
    schedule=jobs.CronSchedule(quartz_cron_expression=CRON, timezone_id=FUSO, pause_status=jobs.PauseStatus.UNPAUSED),
    max_concurrent_runs=1,
    timeout_seconds=7200,
    email_notifications=jobs.JobEmailNotifications(on_failure=[w.current_user.me().user_name]),
    tags={"projeto": "economia", "fonte": "bcb"},
)

for anterior in w.jobs.list(name=NOME_JOB_ANTERIOR):
    w.jobs.delete(job_id=anterior.job_id)

existentes = list(w.jobs.list(name=NOME_JOB))
if existentes:
    job_id = existentes[0].job_id
    w.jobs.reset(job_id=job_id, new_settings=jobs.JobSettings(**parametros))
else:
    job_id = w.jobs.create(**parametros).job_id

print(f"Job {NOME_JOB} (id {job_id}) configurado: {CRON} em {FUSO}")

# COMMAND ----------

execucao = w.jobs.run_now(job_id=job_id).result(timeout=timedelta(minutes=60))

display(
    spark.createDataFrame(
        [
            (t.task_key, t.state.result_state.value, t.execution_duration // 1000 if t.execution_duration else None)
            for t in execucao.tasks
        ],
        "tarefa STRING, resultado STRING, duracao_segundos LONG",
    )
)

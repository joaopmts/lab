"""Verificação estática dos DAGs do lab. Sai com código 1 se encontrar problema.

Uso (com o lab rodando):
    docker compose exec airflow-scheduler python /opt/airflow/tests/check_dags.py [pasta_de_dags]

Checa:
  1. erros de import (DagBag);
  2. todo DAG tem tags;
  3. catchup desligado (ligar gera uma run por intervalo desde start_date);
  4. templates que usam data_interval_*/ds/logical_date direto: no Airflow 3 esses
     valores não existem em runs manuais e o template quebra. Use
     ``dag_run.data_interval_start or dag_run.run_after``.
"""
import re
import sys

from airflow.models.dagbag import DagBag

# Variáveis de contexto que ficam indefinidas em runs manuais no Airflow 3
UNSAFE_VARS = r"(?<![\w.])(data_interval_start|data_interval_end|logical_date|ds|ds_nodash|ts|ts_nodash)(?![\w])"
JINJA_EXPR = re.compile(r"{{(.*?)}}", re.S)


def iter_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from iter_strings(v)
    elif isinstance(value, (list, tuple, set)):
        for v in value:
            yield from iter_strings(v)


def main() -> int:
    folder = sys.argv[1] if len(sys.argv) > 1 else None
    bag = DagBag(dag_folder=folder, include_examples=False)
    problems = []

    for path, err in bag.import_errors.items():
        problems.append(f"[import] {path}: {err.strip().splitlines()[-1]}")

    for dag_id, dag in sorted(bag.dags.items()):
        if not dag.tags:
            problems.append(f"[tags] {dag_id}: sem tags")
        if dag.catchup:
            problems.append(f"[catchup] {dag_id}: catchup=True")
        for task in dag.tasks:
            for field in task.template_fields:
                for s in iter_strings(getattr(task, field, None)):
                    for expr in JINJA_EXPR.findall(s):
                        m = re.search(UNSAFE_VARS, expr)
                        if m:
                            problems.append(
                                f"[template] {dag_id}.{task.task_id}.{field}: usa '{m.group(1)}' direto "
                                f"(indefinido em runs manuais) -> {{{{{expr.strip()}}}}}"
                            )

    print(f"{len(bag.dags)} DAGs verificados")
    for p in problems:
        print("  ✗", p)
    if problems:
        print(f"{len(problems)} problema(s)")
        return 1
    print("  ✓ nenhum problema encontrado")
    return 0


if __name__ == "__main__":
    sys.exit(main())

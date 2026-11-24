"""Intent classifier and dynamic thinking-budget allocator for AIOps queries and alerts."""

from enum import Enum
from typing import Tuple


class QuestionClass(str, Enum):
    INVESTIGACAO_HOST = "investigacao_host"
    POR_QUE_ALERTANDO = "por_que_alertando"
    STATUS_GERAL = "status_geral"
    ANOMALIA = "anomalia"
    TOP_CONSUMIDORES = "top_consumidores"
    MANUTENCAO = "manutencao"
    LOGS = "logs"
    LATENCIA = "latencia"
    ERROS = "erros"
    TRACE = "trace"
    METRICAS = "metricas"
    POST_MORTEM = "post_mortem"
    CAPACITY = "capacity"
    CORRELACAO_CROSS_HOST = "correlacao_cross_host"
    SEGURANCA = "seguranca"
    GENERICA = "generica"


# Priority order: first match wins. Supports both English and Portuguese trigger/query terms.
PRIORITY_KEYWORDS = [
    (QuestionClass.ANOMALIA, ["anomal", "anômal", "fora do padrão", "out of baseline", "deviation", "desvio"]),
    (QuestionClass.SEGURANCA, [
        "/etc/passwd", "/etc/ld.so.preload", "has been changed", "foi alterad",
        "checksum", "integridade", "file integrity", "snort", "suricata", "intrus",
        "unauthorized", "não autorizado", "brute force", "força bruta", "rootkit",
        "tamper", "sudoers", "auth fail", "falha de autentic",
    ]),
    (QuestionClass.POST_MORTEM, [
        "post-mortem", "postmortem", "o que causou", "what caused", "o que aconteceu",
        "what happened", "histórico do incidente", "rca", "root cause", "causa raiz", "análise do evento",
    ]),
    (QuestionClass.CORRELACAO_CROSS_HOST, [
        "vários hosts", "multiple hosts", "cascata", "cascade", "simultâneo",
        "simultaneous", "ao mesmo tempo", "múltiplos",
    ]),
    (QuestionClass.STATUS_GERAL, [
        "status geral", "overall status", "visão geral", "overview",
        "quais problemas", "active problems", "resumo", "summary", "como está",
    ]),
    (QuestionClass.MANUTENCAO, [
        "criar manutenção", "create maintenance", "listar manutenç", "list maintenance",
        "remover manutenç", "delete maintenance", "colocar em manutenção",
    ]),
    (QuestionClass.CAPACITY, [
        "capacidade", "capacity", "disk forecast", "quanto tempo de disco",
        "tendência de crescimento", "growth trend", "esgotar", "run out",
    ]),
    (QuestionClass.TOP_CONSUMIDORES, [
        "top hosts", "quem usa mais", "highest usage", "maior consumo", "ranking",
    ]),
    (QuestionClass.POR_QUE_ALERTANDO, [
        "por que disparou", "why did", "why is", "por que está em problema",
        "explica o alerta", "explain alert",
    ]),
    (QuestionClass.LATENCIA, ["latência", "latency", "p95", "p99", "operações lentas", "slow operations", "lentidão"]),
    (QuestionClass.ERROS, ["taxa de erros", "error rate", "traces com erro", "falhas no", "5xx"]),
    (QuestionClass.LOGS, ["logs do", "logs de ", "logs for", "o que apareceu nos logs"]),
    (QuestionClass.TRACE, ["trace ", "traceid", "trace_id"]),
    (QuestionClass.METRICAS, ["métrica ", "metric "]),
    (QuestionClass.INVESTIGACAO_HOST, [
        "investiga", "investigate", "o que está errado", "what is wrong",
        "status do", "status of", "problema no", "hypervisor",
        " is down", " down", "link down", "port ", "porta ",
        "sites fora", "fora do ar", "unreachable", "indisponív", "indisponiv",
        "perda de pacote", "packet loss", "timeout", "serviço inativo", "service down",
    ]),
]

THINKING_BUDGET_BY_CLASS = {
    QuestionClass.STATUS_GERAL: 512,
    QuestionClass.MANUTENCAO: 256,
    QuestionClass.LOGS: 1024,
    QuestionClass.METRICAS: 512,
    QuestionClass.TRACE: 1024,
    QuestionClass.POR_QUE_ALERTANDO: 1024,
    QuestionClass.TOP_CONSUMIDORES: 1024,
    QuestionClass.LATENCIA: 2048,
    QuestionClass.ERROS: 2048,
    QuestionClass.INVESTIGACAO_HOST: 2048,
    QuestionClass.ANOMALIA: 4096,
    QuestionClass.CAPACITY: 4096,
    QuestionClass.POST_MORTEM: 8192,
    QuestionClass.CORRELACAO_CROSS_HOST: 8192,
    QuestionClass.SEGURANCA: 4096,
    QuestionClass.GENERICA: 2048,
}


def classify(question: str, has_active_incident: bool = False) -> Tuple[QuestionClass, int]:
    """Classify a query/trigger into a QuestionClass and return its thinking budget."""
    q = (question or "").lower()
    for question_class, keywords in PRIORITY_KEYWORDS:
        if any(k in q for k in keywords):
            budget = THINKING_BUDGET_BY_CLASS[question_class]
            if has_active_incident and budget < 4096:
                budget = 4096
            return question_class, budget
    budget = THINKING_BUDGET_BY_CLASS[QuestionClass.GENERICA]
    if has_active_incident and budget < 4096:
        budget = 4096
    return QuestionClass.GENERICA, budget

"""Tests for intent classifier (EN + PT-BR) and active-incident budget bump."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from classifier import QuestionClass, classify  # noqa: E402


def test_classifier_classes():
    cls, budget = classify("What are the active problems overall?", has_active_incident=False)
    assert cls == QuestionClass.STATUS_GERAL and budget == 512

    cls2, budget2 = classify("Possible rootkit detected on host", has_active_incident=True)
    assert cls2 == QuestionClass.SEGURANCA and budget2 >= 4096

    cls3, budget3 = classify("High p95 latency on api-checkout", has_active_incident=False)
    assert cls3 == QuestionClass.LATENCIA and budget3 == 2048

    cls4, budget4 = classify("Port 25 is down on switch-core-01", has_active_incident=True)
    assert cls4 == QuestionClass.INVESTIGACAO_HOST and budget4 >= 4096

    cls5, budget5 = classify("Something weird without keywords", has_active_incident=True)
    assert cls5 == QuestionClass.GENERICA and budget5 == 4096


if __name__ == "__main__":
    test_classifier_classes()
    print("ok: test_classifier")

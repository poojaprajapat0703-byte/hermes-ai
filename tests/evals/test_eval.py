"""
tests/evals/test_eval.py
─────────────────────────
Tests for the eval harness itself.

Why test the eval harness?
  The eval harness is code too — it can have bugs.
  We test it in mock mode (fake LLM = perfect answers)
  to verify the scoring logic is correct before
  trusting it to grade the real AI.

Test strategy:
  - Mock mode tests → verify scoring math
  - These run in CI without Ollama (fast, free)
  - Real LLM tests → skipped in CI, run manually
"""

import pytest


class TestEvalHarness:

    def test_mock_eval_scores_100_percent(self):
        """
        In mock mode the AI always returns correct answers.
        So accuracy must be 100%.

        This tests that the scoring math is correct.
        If this fails, the harness itself has a bug.
        """
        from tests.evals.eval_harness import run_eval

        report = run_eval(use_mock=True)

        assert report.total == 10, "Must evaluate all 10 incidents"
        assert report.severity_accuracy == 1.0, "Mock should get 100% severity"
        assert report.domain_accuracy == 1.0, "Mock should get 100% domain"
        assert report.overall_accuracy == 1.0, "Mock should get 100% overall"
        assert report.passed is True, "Mock eval must pass threshold"

    def test_golden_dataset_has_10_incidents(self):
        """Golden dataset must have exactly 10 incidents."""
        from tests.evals.golden_dataset import get_dataset

        dataset = get_dataset()
        assert len(dataset) == 10

    def test_golden_dataset_has_required_fields(self):
        """Every incident must have all required fields."""
        from tests.evals.golden_dataset import get_dataset

        required_fields = {"id", "incident", "expected_severity", "expected_domain", "notes"}
        dataset = get_dataset()

        for item in dataset:
            for field in required_fields:
                assert field in item, f"Incident {item.get('id')} missing field: {field}"

    def test_golden_dataset_severities_are_valid(self):
        """All expected severities must be valid values."""
        from tests.evals.golden_dataset import get_dataset

        valid_severities = {"low", "medium", "high", "critical"}
        dataset = get_dataset()

        for item in dataset:
            assert item["expected_severity"] in valid_severities, \
                f"Incident {item['id']} has invalid severity: {item['expected_severity']}"

    def test_golden_dataset_domains_are_valid(self):
        """All expected domains must be valid values."""
        from tests.evals.golden_dataset import get_dataset

        valid_domains = {"frontend", "backend", "database", "infrastructure", "network", "unknown"}
        dataset = get_dataset()

        for item in dataset:
            assert item["expected_domain"] in valid_domains, \
                f"Incident {item['id']} has invalid domain: {item['expected_domain']}"

    def test_eval_report_accuracy_threshold(self):
        """Accuracy threshold must be 0.70."""
        from tests.evals.eval_harness import ACCURACY_THRESHOLD

        assert ACCURACY_THRESHOLD == 0.70

    def test_eval_result_passed_property(self):
        """EvalReport.passed must be True only when accuracy >= 70%."""
        from tests.evals.eval_harness import EvalReport

        # 7/10 correct = 70% = exactly at threshold = PASS
        report = EvalReport(total=10, severity_correct=7, domain_correct=7)
        assert report.passed is True

        # 6/10 correct = 60% = below threshold = FAIL
        report2 = EvalReport(total=10, severity_correct=6, domain_correct=6)
        assert report2.passed is False


@pytest.mark.integration
class TestEvalWithRealLLM:
    """
    Real LLM eval tests.
    Requires Ollama running with llama3.2.
    Run with: pytest tests/evals/test_eval.py -m integration -v
    """

    def test_real_classifier_meets_accuracy_threshold(self):
        """Real classifier must score >= 70% on golden dataset."""
        from tests.evals.eval_harness import run_eval

        report = run_eval(use_mock=False)
        report.print_report()

        assert report.passed, (
            f"Classifier accuracy {report.overall_accuracy:.1%} "
            f"is below threshold {0.70:.1%}. "
            f"Severity: {report.severity_accuracy:.1%}, "
            f"Domain: {report.domain_accuracy:.1%}"
        )

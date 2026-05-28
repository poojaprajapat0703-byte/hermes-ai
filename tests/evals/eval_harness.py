"""
tests/evals/eval_harness.py
─────────────────────────────
What is this file?
  This is the GRADER. It:
    1. Takes each incident from the golden dataset
    2. Runs it through the classifier (the AI)
    3. Compares the AI's answer to the expected answer
    4. Calculates a score (how many did the AI get right?)
    5. FAILS if the score is below 70%

Why 70% threshold?
  A random guesser would get ~25% right (4 severity options).
  70% means the AI is genuinely learning patterns, not guessing.
  In production you'd raise this to 90%+ as the model improves.

What is accuracy?
  accuracy = (number of correct answers) / (total questions) × 100

  Example:
    10 incidents, AI got 8 right → accuracy = 80% → PASS ✅
    10 incidents, AI got 5 right → accuracy = 50% → FAIL ❌

What counts as "correct"?
  We score severity and domain separately:
    severity_accuracy = correct severities / total
    domain_accuracy   = correct domains / total
    overall_accuracy  = (severity_accuracy + domain_accuracy) / 2

  Why separate scores?
    Severity wrong but domain right is still useful info.
    Separate scores tell you WHERE the AI is struggling.
"""

import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# The minimum accuracy required to pass
# Below this → CI fails
ACCURACY_THRESHOLD = 0.70


@dataclass
class EvalResult:
    """
    Result for a single incident evaluation.

    What is a dataclass?
      A dataclass is like a TypedDict but a Python class.
      It automatically creates __init__, __repr__ etc.
      We use it to store one incident's evaluation result cleanly.
    """
    incident_id: int
    incident_text: str
    expected_severity: str
    expected_domain: str
    predicted_severity: str
    predicted_domain: str
    severity_correct: bool
    domain_correct: bool
    latency_seconds: float
    error: str | None = None


@dataclass
class EvalReport:
    """
    Summary report for all incidents evaluated.

    Contains per-incident results and overall accuracy scores.
    """
    results: list[EvalResult] = field(default_factory=list)
    total: int = 0
    severity_correct: int = 0
    domain_correct: int = 0

    @property
    def severity_accuracy(self) -> float:
        """What % of severities did the AI get right?"""
        if self.total == 0:
            return 0.0
        return self.severity_correct / self.total

    @property
    def domain_accuracy(self) -> float:
        """What % of domains did the AI get right?"""
        if self.total == 0:
            return 0.0
        return self.domain_correct / self.total

    @property
    def overall_accuracy(self) -> float:
        """Average of severity and domain accuracy."""
        return (self.severity_accuracy + self.domain_accuracy) / 2

    @property
    def passed(self) -> bool:
        """Did we hit the 70% threshold?"""
        return self.overall_accuracy >= ACCURACY_THRESHOLD

    def print_report(self) -> None:
        """Print a human-readable report to the console."""
        print("\n" + "═" * 60)
        print("  HERMES CLASSIFIER EVAL REPORT")
        print("═" * 60)
        print(f"  Total incidents evaluated : {self.total}")
        print(f"  Severity accuracy         : {self.severity_accuracy:.1%}")
        print(f"  Domain accuracy           : {self.domain_accuracy:.1%}")
        print(f"  Overall accuracy          : {self.overall_accuracy:.1%}")
        print(f"  Threshold                 : {ACCURACY_THRESHOLD:.1%}")
        print(f"  Result                    : {'✅ PASS' if self.passed else '❌ FAIL'}")
        print("═" * 60)

        print("\n  Per-incident breakdown:")
        print(f"  {'ID':<4} {'SEV':^6} {'DOM':^10} {'PRED_SEV':^10} {'PRED_DOM':^12} {'OK?'}")
        print("  " + "-" * 52)

        for r in self.results:
            sev_ok = "✅" if r.severity_correct else "❌"
            dom_ok = "✅" if r.domain_correct else "❌"
            ok = "✅" if r.severity_correct and r.domain_correct else "❌"
            print(
                f"  {r.incident_id:<4} "
                f"{r.expected_severity:^6} "
                f"{r.expected_domain:^10} "
                f"{r.predicted_severity:^10} "
                f"{r.predicted_domain:^12} "
                f"{ok} (sev:{sev_ok} dom:{dom_ok})"
            )
            if r.error:
                print(f"       ERROR: {r.error}")

        print("═" * 60 + "\n")


def run_eval(use_mock: bool = False) -> EvalReport:
    """
    Run the full evaluation against the golden dataset.

    Args:
      use_mock: If True, use a fake LLM (for CI without Ollama).
                If False, use the real classifier with real LLM.

    Returns:
      EvalReport with all results and accuracy scores.

    How it works:
      1. Load golden dataset (10 incidents)
      2. For each incident:
         a. Run through classifier node
         b. Compare result to expected answer
         c. Record result
      3. Calculate overall accuracy
      4. Return report
    """
    from services.orchestrator.nodes.classifier import classify_incident
    from tests.evals.golden_dataset import get_dataset

    dataset = get_dataset()
    report = EvalReport()

    logger.info("Starting eval on %d incidents (mock=%s)", len(dataset), use_mock)

    for item in dataset:
        incident_text = item["incident"]
        expected_severity = item["expected_severity"]
        expected_domain = item["expected_domain"]

        start_time = time.time()
        error = None

        if use_mock:
            # In mock mode: pretend the AI always returns the correct answer
            # This is used to test the eval harness LOGIC without a real LLM
            predicted_severity = expected_severity
            predicted_domain = expected_domain
        else:
            # Real mode: run through the actual classifier
            try:
                state = {
                    "incident": incident_text,
                    "classification": {},
                    "analyses": [],
                    "rca_report": {},
                }
                result = classify_incident(state)
                classification = result.get("classification", {})
                predicted_severity = classification.get("severity", "unknown")
                predicted_domain = classification.get("domain", "unknown")

                if "error" in classification:
                    error = classification["error"]

            except Exception as exc:
                logger.error("Eval failed for incident %d: %s", item["id"], exc)
                predicted_severity = "unknown"
                predicted_domain = "unknown"
                error = str(exc)

        latency = time.time() - start_time

        # Compare predictions to expected answers
        severity_correct = predicted_severity == expected_severity
        domain_correct = predicted_domain == expected_domain

        eval_result = EvalResult(
            incident_id=item["id"],
            incident_text=incident_text[:80] + "...",
            expected_severity=expected_severity,
            expected_domain=expected_domain,
            predicted_severity=predicted_severity,
            predicted_domain=predicted_domain,
            severity_correct=severity_correct,
            domain_correct=domain_correct,
            latency_seconds=latency,
            error=error,
        )

        report.results.append(eval_result)
        report.total += 1
        if severity_correct:
            report.severity_correct += 1
        if domain_correct:
            report.domain_correct += 1

        logger.info(
            "Incident %d: sev=%s(%s) dom=%s(%s) latency=%.2fs",
            item["id"],
            predicted_severity,
            "✅" if severity_correct else "❌",
            predicted_domain,
            "✅" if domain_correct else "❌",
            latency,
        )

    report.print_report()
    return report


if __name__ == "__main__":
    """
    Run the eval directly:
      python -m tests.evals.eval_harness

    Uses real LLM. Make sure Ollama is running.
    """
    import sys
    logging.basicConfig(level=logging.INFO)

    report = run_eval(use_mock=False)

    # Exit with error code if accuracy below threshold
    # This is what CI uses to know if the eval passed or failed
    sys.exit(0 if report.passed else 1)

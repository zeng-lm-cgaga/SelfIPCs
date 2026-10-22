import ast
from pathlib import Path
import re
import unittest


REPO = Path(__file__).resolve().parents[2]
CANDIDATE = "linux-notification-20261021"
CANDIDATE_REF = "refs/heads/" + CANDIDATE


def block(text, name, indentation):
    match = re.search(
        rf"(?ms)^{' ' * indentation}{re.escape(name)}:\n(.*?)(?=^ {{0,{indentation}}}\S|\Z)",
        text)
    if match is None:
        raise AssertionError("Missing workflow block: " + name)
    return match.group(1)


def full_enabled(expression, event, ref, full=None):
    context = {"inputs": {"full": full}, "github": {"event_name": event, "ref": ref}}
    parsed = ast.parse(expression.replace("||", " or ").replace("&&", " and "),
                       mode="eval")

    def evaluate(node):
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (str, bool)):
            return node.value
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            return context[node.value.id][node.attr]
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            return any(evaluate(value) for value in node.values)
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.And):
            return all(evaluate(value) for value in node.values)
        if isinstance(node, ast.Compare) and len(node.ops) == 1 and isinstance(node.ops[0], ast.Eq):
            return evaluate(node.left) == evaluate(node.comparators[0])
        raise AssertionError("Unsupported workflow condition: " + ast.dump(node))

    return bool(evaluate(parsed))


class CiWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = (REPO / ".github/workflows/ci.yml").read_text()
        cls.events = block(cls.workflow, "on", 0)
        cls.audit = block(cls.workflow, "release-audit", 2)
        environment = block(cls.audit, "env", 4)
        condition = re.fullmatch(r"      SELFIPCS_FULL: \$\{\{ (.+) \}\}\n", environment)
        if condition is None:
            raise AssertionError("Expected one job-scoped SELFIPCS_FULL condition")
        cls.condition = condition.group(1)

    def test_only_main_and_exact_candidate_pushes_trigger(self):
        push = block(self.events, "push", 2)
        self.assertEqual(push.strip(), f"branches: [main, {CANDIDATE}]")

    def test_existing_events_and_input_defaults_are_preserved(self):
        self.assertEqual(set(re.findall(r"^  ([a-z_]+):$", self.events, re.MULTILINE)),
                         {"push", "pull_request", "workflow_dispatch", "workflow_call", "schedule"})
        self.assertEqual(block(self.events, "pull_request", 2), "")
        for event, default in [("workflow_dispatch", "true"), ("workflow_call", "false")]:
            with self.subTest(event=event):
                inputs = block(self.events, event, 2)
                self.assertIn("        type: boolean\n", inputs)
                self.assertIn(f"        default: {default}\n", inputs)
        self.assertEqual(block(self.events, "schedule", 2).strip(), "- cron: '17 3 * * 1'")

    def test_candidate_push_always_enables_full(self):
        for full in [None, False, True]:
            with self.subTest(full=full):
                self.assertTrue(full_enabled(self.condition, "push", CANDIDATE_REF, full))

    def test_other_push_refs_do_not_force_full(self):
        for ref in ["refs/heads/main", CANDIDATE_REF + "-extra",
                    "refs/heads/codex/" + CANDIDATE, "refs/heads/feature",
                    "refs/tags/" + CANDIDATE]:
            with self.subTest(ref=ref):
                self.assertFalse(full_enabled(self.condition, "push", ref))

    def test_main_and_pull_request_defaults_remain_nonfull(self):
        for event, ref in [("push", "refs/heads/main"), ("pull_request", "refs/pull/17/merge"),
                           ("pull_request", CANDIDATE_REF)]:
            with self.subTest(event=event, ref=ref):
                self.assertFalse(full_enabled(self.condition, event, ref))

    def test_dispatch_and_reusable_inputs_remain_authoritative(self):
        for event in ["workflow_dispatch", "workflow_call"]:
            for ref in ["refs/heads/main", CANDIDATE_REF, "refs/tags/v0.5.0"]:
                for full in [False, True]:
                    with self.subTest(event=event, ref=ref, full=full):
                        self.assertEqual(full_enabled(self.condition, event, ref, full), full)
        for full in [False, True]:
            with self.subTest(reusable_release_push_full=full):
                self.assertEqual(full_enabled(self.condition, "push", "refs/tags/v0.5.0", full),
                                 full)

    def test_schedule_remains_full(self):
        for full in [None, False, True]:
            with self.subTest(full=full):
                self.assertTrue(full_enabled(self.condition, "schedule", "refs/heads/main", full))

    def test_full_flag_reaches_both_release_audit_runners(self):
        self.assertIn("        os: [ubuntu-22.04, ubuntu-24.04]\n", self.audit)
        self.assertIn("    runs-on: ${{ matrix.os }}\n", self.audit)
        self.assertIn('if [[ "$SELFIPCS_FULL" == true ]]; then args+=(--full); fi', self.audit)
        self.assertIn('bash scripts/ci.sh "${args[@]}"', self.audit)
        self.assertEqual(self.workflow.count("SELFIPCS_FULL:"), 1)


if __name__ == "__main__":
    unittest.main()

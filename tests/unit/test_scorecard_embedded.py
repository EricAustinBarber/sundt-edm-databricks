import ast
from pathlib import Path


def _load_embedded_definitions():
    notebook_path = Path("databricks/resources/notebooks/maturity/scorecard_evaluation_notebook.py")
    source = notebook_path.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "embedded_definitions":
                    return ast.literal_eval(node.value)
    raise AssertionError("embedded_definitions not found in scorecard_evaluation_notebook.py")


def test_embedded_scorecard_weights_sum_to_100():
    defs = _load_embedded_definitions()
    weights = [item["weight"] for item in defs]
    assert sum(weights) == 100, f"embedded_definitions weights must sum to 100, got {sum(weights)}"


def test_embedded_scorecard_check_ids_unique():
    defs = _load_embedded_definitions()
    ids = [item["check_id"] for item in defs]
    assert len(ids) == len(set(ids)), "embedded_definitions has duplicate check_id values"


def test_embedded_scorecard_required_fields_present():
    defs = _load_embedded_definitions()
    required = {"check_id", "dimension", "check_name", "weight"}
    for item in defs:
        missing = required - set(item.keys())
        assert not missing, f"embedded_definitions missing required fields: {sorted(missing)}"
        for key in required:
            assert item[key] is not None and str(item[key]).strip() != "", (
                f"embedded_definitions has empty required field: {key}"
            )

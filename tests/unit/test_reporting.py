from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from sundt_edm_quality.reporting import _build_report_payload, _render_markdown


def test_build_report_payload_summarizes_governance_sources():
    payload = _build_report_payload(
        catalog="dev",
        databricks_env="dev",
        raw_summary={
            "bigeye_monitors": {"row_count": 12, "max_ingested_at": "2026-03-11 08:00:00"},
            "alation_assets": {"row_count": 7, "max_ingested_at": "2026-03-11 08:05:00"},
        },
        dataset_detail=[
            {
                "report_date": "2026-03-11",
                "dataset_id": "1",
                "dataset_name": "finance.ap_invoice",
                "domain_name": "Finance",
                "bigeye_monitor_count": 2,
                "alert_count_7d": 1,
                "alation_endorsed": True,
                "alation_owner": "owner_a",
                "alation_steward": "steward_a",
                "alation_lineage_present": True,
                "dataset_score": 92.0,
                "dataset_status": "Green",
            },
            {
                "report_date": "2026-03-11",
                "dataset_id": "2",
                "dataset_name": "hr.employee_master",
                "domain_name": "HR",
                "bigeye_monitor_count": 0,
                "alert_count_7d": 4,
                "alation_endorsed": False,
                "alation_owner": "owner_b",
                "alation_steward": None,
                "alation_lineage_present": False,
                "dataset_score": 65.0,
                "dataset_status": "Red",
            },
        ],
        domain_summary=[
            {
                "report_date": "2026-03-11",
                "domain_name": "HR",
                "domain_score": 65.0,
                "domain_status": "Red",
                "dataset_count": 1,
            },
            {
                "report_date": "2026-03-11",
                "domain_name": "Finance",
                "domain_score": 92.0,
                "domain_status": "Green",
                "dataset_count": 1,
            },
        ],
        metric_summary=[
            {
                "report_date": "2026-03-11",
                "metric_id": "GOV_ENDORSEMENT",
                "metric_name": "alation_endorsed",
                "avg_metric_score": 70.0,
                "dataset_count": 2,
            }
        ],
        maturity_result={
            "collected_at": "2026-03-11T08:10:00Z",
            "env": "dev",
            "run_id": "123",
            "commit_sha": "abc123",
            "total_score": 76.5,
            "overall_status": "Ready",
            "blocked_reasons": ["none"],
            "warned_reasons": ["1 checks missing status"],
        },
        maturity_dimensions=[
            {
                "dimension": "Security",
                "dimension_score": 20.0,
                "pass_count": 1,
                "partial_count": 0,
                "fail_count": 0,
                "unknown_count": 0,
            }
        ],
        maturity_checks=[
            {
                "check_id": "WH-01",
                "check_name": "Warehouse queried on at least ten distinct days in the last 30 days",
                "status_norm": "Pass",
                "weight": 12,
            },
            {
                "check_id": "WH-08",
                "check_name": "Table comment coverage demonstrates documented warehouse assets",
                "status_norm": "Partial",
                "weight": 14,
                "notes": "Coverage below target",
            },
        ],
        warnings=[],
        top_n=5,
    )

    assert payload["sources"]["bigeye"]["raw_row_count"] == 12
    assert payload["sources"]["bigeye"]["monitored_dataset_count"] == 1
    assert payload["sources"]["bigeye"]["monitored_dataset_pct"] == 50.0
    assert payload["sources"]["alation"]["endorsed_dataset_count"] == 1
    assert payload["sources"]["alation"]["owner_steward_complete_count"] == 1
    assert payload["sources"]["alation"]["lineage_present_count"] == 1
    assert payload["sources"]["databricks_governance"]["avg_dataset_score"] == 78.5
    assert payload["sources"]["databricks_governance"]["status_counts"] == {
        "Green": 1,
        "Yellow": 0,
        "Red": 1,
    }
    assert payload["sources"]["databricks_maturity"]["total_score"] == 76.5
    assert len(payload["sources"]["databricks_maturity"]["non_passing_checks"]) == 1


def test_render_markdown_includes_review_sections_and_warnings():
    payload = _build_report_payload(
        catalog="dev",
        databricks_env="dev",
        raw_summary={
            "bigeye_monitors": {"row_count": 2, "max_ingested_at": "2026-03-11 08:00:00"},
            "alation_assets": {"row_count": 2, "max_ingested_at": "2026-03-11 08:05:00"},
        },
        dataset_detail=[
            {
                "report_date": "2026-03-11",
                "dataset_id": "1",
                "dataset_name": "finance.ap_invoice",
                "domain_name": "Finance",
                "bigeye_monitor_count": 2,
                "alert_count_7d": 0,
                "alation_endorsed": True,
                "alation_owner": "owner_a",
                "alation_steward": "steward_a",
                "alation_lineage_present": True,
                "dataset_score": 95.0,
                "dataset_status": "Green",
            }
        ],
        domain_summary=[
            {
                "report_date": "2026-03-11",
                "domain_name": "Finance",
                "domain_score": 95.0,
                "domain_status": "Green",
                "dataset_count": 1,
            }
        ],
        metric_summary=[],
        maturity_result={
            "collected_at": "2026-03-11T08:10:00Z",
            "env": "dev",
            "run_id": "123",
            "commit_sha": "abc123",
            "total_score": 95.0,
            "overall_status": "Ready",
            "blocked_reasons": [],
            "warned_reasons": [],
        },
        maturity_dimensions=[],
        maturity_checks=[],
        warnings=["databricks maturity checks: table missing"],
        top_n=3,
    )

    rendered = _render_markdown(payload, top_n=3)

    assert "# Data Governance and Maturity Review" in rendered
    assert "## Bigeye" in rendered
    assert "## Alation" in rendered
    assert "## Databricks Governance Scorecard" in rendered
    assert "## Databricks Environment Maturity" in rendered
    assert "## Warnings" in rendered
    assert "table missing" in rendered

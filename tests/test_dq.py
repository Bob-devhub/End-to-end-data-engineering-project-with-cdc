import pytest

from cdc_framework.config import DqRule
from cdc_framework.dq import DqResult, rule_to_predicate


@pytest.mark.parametrize(
    ("rule", "expected"),
    [
        ({"rule_type": "not_null", "column": "id"}, "`id` IS NOT NULL"),
        ({"rule_type": "range", "column": "qty", "min": 1, "max": 10},
         "(`qty` IS NULL OR (`qty` >= 1.0 AND `qty` <= 10.0))"),
        ({"rule_type": "range", "column": "qty", "min": 0}, "(`qty` IS NULL OR (`qty` >= 0.0))"),
        ({"rule_type": "regex", "column": "code", "pattern": "^[A-Z]{3}$"},
         "(`code` IS NULL OR `code` RLIKE '^[A-Z]{3}$')"),
        ({"rule_type": "accepted_values", "column": "region", "values": ["North", "O'Neil"]},
         "(`region` IS NULL OR `region` IN ('North', 'O\\'Neil'))"),
    ],
)
def test_rule_to_predicate(rule, expected):
    assert rule_to_predicate(DqRule(**rule)) == expected


def test_unique_has_no_row_predicate():
    assert rule_to_predicate(DqRule(rule_type="unique", column="id")) is None


def test_range_requires_bounds():
    with pytest.raises(ValueError):
        DqRule(rule_type="range", column="x")


def test_threshold_breach():
    assert DqResult("r", "fail", failed_rows=1, total_rows=100, threshold=0.0).breached
    assert not DqResult("r", "fail", failed_rows=1, total_rows=100, threshold=0.05).breached
    assert not DqResult("r", "fail", failed_rows=0, total_rows=0, threshold=0.0).breached

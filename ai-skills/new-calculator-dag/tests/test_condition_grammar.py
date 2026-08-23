import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from condition_grammar import check_condition  # noqa: E402


def test_valid_two_segment():
    assert check_condition("CALC.CAPITALCALC") == []

def test_valid_three_segment_region():
    assert check_condition("SOURCE.MERIVAL.USRG") == []

def test_valid_ident_with_underscores():
    assert check_condition("SOURCE.AQUA_RISK_PLATFORM") == []

def test_unknown_type_is_error():
    errs = check_condition("EVENT.MERIVAL.USRG")
    assert any("unknown condition type" in e.message for e in errs)
    assert all(e.severity == "ERROR" for e in errs)

def test_lowercase_is_error():
    errs = check_condition("source.merival.usrg")
    assert any(e.severity == "ERROR" for e in errs)

def test_underscore_where_dot_expected_is_error():
    # The documented drift class: SOURCE.MERIVAL_AMER when the registry uses SOURCE.MERIVAL.AMER
    errs = check_condition("SOURCE.MERIVAL_AMER", known_idents={"MERIVAL"})
    assert any("did you mean" in e.message for e in errs)

def test_unknown_region_is_warning_not_error():
    errs = check_condition("SOURCE.MERIVAL.XXXX")
    assert errs and all(e.severity == "WARN" for e in errs)

def test_single_segment_is_error():
    assert any(e.severity == "ERROR" for e in check_condition("CAPITALCALC"))

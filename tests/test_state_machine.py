from slidemodel.state.machine import next_state
from slidemodel.models.signals import EvaluationInput, ConditionFlags
from slidemodel.models.types import ModelState

def mk(inp_state, c1, c2, c3, c4, in_scope=True, has_data=True):
    return EvaluationInput(
        in_scope=in_scope,
        has_sufficient_data=has_data,
        prev_state=inp_state,
        flags=ConditionFlags(condition_1=c1, condition_2=c2, condition_3=c3, condition_4=c4),
    )

def test_out_of_scope_is_ignore():
    assert next_state(mk("MONITOR", True, True, True, True, in_scope=False)) == ModelState.IGNORE

def test_no_data_is_monitor():
    assert next_state(mk("MONITOR", True, True, True, True, has_data=False)) == ModelState.MONITOR

def test_disqualifier_blocks_escalation():
    assert next_state(mk("MONITOR", False, True, True, True)) == ModelState.MONITOR

def test_monitor_to_track_requires_c1_c2_c4():
    assert next_state(mk("MONITOR", True, True, False, True)) == ModelState.TRACK
    assert next_state(mk("MONITOR", True, True, False, False)) == ModelState.MONITOR
    assert next_state(mk("MONITOR", True, False, False, True)) == ModelState.MONITOR

def test_track_to_terminal_requires_c3():
    assert next_state(mk("TRACK", True, True, True, True)) == ModelState.TERMINAL
    assert next_state(mk("TRACK", True, True, False, True)) == ModelState.TRACK

def test_terminal_stays_terminal():
    assert next_state(mk("TERMINAL", True, True, True, True)) == ModelState.TERMINAL

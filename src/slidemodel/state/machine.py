from slidemodel.models.types import ModelState
from slidemodel.models.signals import EvaluationInput

def next_state(inp: EvaluationInput) -> ModelState:
    if not inp.in_scope:
        return ModelState.IGNORE
    if not inp.has_sufficient_data:
        return ModelState.MONITOR

    # Hard disqualifier
    if not inp.flags.condition_1:
        return ModelState.MONITOR

    prev = ModelState(inp.prev_state)

    if prev == ModelState.MONITOR:
        if inp.flags.condition_2 and inp.flags.condition_4:
            return ModelState.TRACK
        return ModelState.MONITOR

    if prev == ModelState.TRACK:
        if inp.flags.condition_3:
            return ModelState.TERMINAL
        return ModelState.TRACK

    if prev == ModelState.TERMINAL:
        return ModelState.TERMINAL

    return prev

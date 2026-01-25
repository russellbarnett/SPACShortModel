from pydantic import BaseModel

class ConditionFlags(BaseModel):
    condition_1: bool  # demand constrained (hard disqualifier)
    condition_2: bool  # scale creates overhead
    condition_3: bool  # capital delays discipline
    condition_4: bool  # optics over outcomes (early)

class EvaluationInput(BaseModel):
    in_scope: bool
    has_sufficient_data: bool
    prev_state: str
    flags: ConditionFlags

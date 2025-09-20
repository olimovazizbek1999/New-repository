import re
from services import openai_utils




def test_validate_two_sentence():
good = "We modernize legacy ERP for midsize distributors with careful migrations and staff training. Our platform highlights transparent inventory data so teams act faster and reduce costly fulfillment errors."
assert openai_utils._validate_opener(good)


bad = "Hello there."
assert not openai_utils._validate_opener(bad)


q = "We do X well? Next sentence also has a question?"
assert not openai_utils._validate_opener(q)
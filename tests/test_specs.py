import pytest

from pydap.handlers.sqlalchemy import dds_spec_to_model

@pytest.mark.parametrize('a, b, equal', [
    (False, False, True),
    (False, True, False),
    (True, False, False),
    (True, True, True),
])
def test_models_are_equal(a, b, equal, make_model, models_are_equal):
    model_a = make_model(a)
    model_b = make_model(b)
    assert models_are_equal(model_a, model_b) == equal


@pytest.mark.parametrize('with_attributes, with_data', [
    (False, False),
    (True, True)
])
def test_dds_spec_to_model(
        with_attributes, with_data,
        make_dataset_defn, make_model, models_are_equal
):
    dataset_defn = make_dataset_defn(with_attributes)
    name, declaration = next(iter(dataset_defn.items()))
    model = dds_spec_to_model(name, declaration, {})
    exp_model = make_model(with_attributes)
    assert models_are_equal(model, exp_model)

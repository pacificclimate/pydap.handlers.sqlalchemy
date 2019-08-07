import pytest
from pydap.handlers.sqlalchemy import SQLAlchemyHandler


def test_from_no_file():
    handler = SQLAlchemyHandler(None)
    assert handler.config == {}
    assert handler.dataset is None


def test_from_file(config_file_1, models_are_equal, make_model):
    handler = SQLAlchemyHandler(config_file_1)
    assert models_are_equal(handler.dataset, make_model(False))


def test_update(config_file_1, make_dataset_defn, models_are_equal, make_model):
    config = {
        'database': None,
        'dataset': make_dataset_defn(True)
    }
    handler = SQLAlchemyHandler(config_file_1).update(config)
    assert models_are_equal(handler.dataset, make_model(True))
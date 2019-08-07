import pytest
from pydap.handlers.sqlalchemy import data_source_to_stream, SQLAlchemyData
from helpers.db_model import Fruit


def fruit_query(session):
    return (
        session.query(Fruit.id.label('id'), Fruit.name.label('name'))
        .select_from(Fruit)
        .all()
    )


def test_stuff(database):
    fruits = fruit_query(database['session'])
    fruit = next(iter(fruits))
    print('#### type', type(fruit))
    print('#### dir', dir(fruit))
    print('#### keys', fruit.keys())


@pytest.mark.parametrize('data_source, exp_data', [
    (99, [99]),
    ('giraffe', ['giraffe']),
    ([99, 'giraffe'], [99, 'giraffe']),
    ('SQL{{ SELECT * FROM fruits }}',
     [(1, 'cherimoya'), (2, 'durian'), (3, 'medlar')]),
    (fruit_query,
     [(1, 'cherimoya'), (2, 'durian'), (3, 'medlar')])
])
def test_data_source_to_stream(data_source, exp_data, database):
    data = data_source_to_stream(data_source, {'dsn': database['uri']})
    assert list(data) == exp_data


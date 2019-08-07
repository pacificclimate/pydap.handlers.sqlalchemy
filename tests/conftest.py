import sys
import os
# Make items from helpers/ subdirectory available here.
# See also remark below re. helper function fixtures ... a bit of a fail.
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))

import yaml
import pytest
from tempfile import NamedTemporaryFile

from pydap.model import DatasetType, BaseType, SequenceType

from helpers.db_model import Fruit



# A few interesting things in this conftest:
#
# - Pass helper functions into tests using fixtures. Such a fixture returns
#   a function (e.g., a special data comparison function) which is used by the
#   test. This is an alternative to a couple of other ways to provide helper
#   functions, both of which have some downsides for pytest test code in
#   particular. The fixture method is a little clunky, but it is
#   straightforward. The convention used here is to name the fixture `xyz` and
#   the function it supplies `_xyz`.
#   Later: This worked in a lot of cases, but not in some. Ended up having
#   to do the ugly to import some helper classes, etc. into tests. Neither of
#   these ways is particularly elegant.
#
# - *Avoid* using the pytest `indirect`
#   (`see <https://docs.pytest.org/en/latest/example/parametrize.html#apply-indirect-on-particular-arguments>`_
#   to parametrize fixtures.
#   Past experience has shown that this feature is cool and useful,
#   but also a bit clunky, hard to read, and not suited to all use cases.
#   (In particular, it doesn't work well if you wish to use the same fixture
#   twice in a test, e.g., a data-generating fixture with a parameter that
#   controls the data generated.) Instead, we use the helper-function method
#   above to pass in the data generating function, and invoke it in the test
#   function with arguments usually derived directly from parameters to the
#   test.
#
# - Use, at least on a provisional try-out basis, the relatively new database
#   testing package
#   `pytest-pgsql <https://pypi.org/project/pytest-pgsql/>`_.
#   It's based the tried, true, and familiar package `testing.postgresql`,
#   so this looks like a safe bet.


# Common helpers

def attributes(prefix, count=1):
    return {
        '{}{}'.format(prefix, i): '{}{}_value'.format(prefix, i)
        for i in range(count)
    }


# Model fixtures

def _make_model(with_attributes=False, with_data=False):
    def maybe(attributes):
        return attributes if with_attributes else None

    model = DatasetType(
        name='dataset_name',
        attributes=maybe(attributes('ds', 1))
    )
    model['sequence_name'] = SequenceType(
        name='sequence_name',
        attributes=maybe(attributes('seq', 2))
    )
    for name in ['a', 'b', 'c']:
        model['sequence_name'][name] = BaseType(
            name=name,
            attributes=maybe(attributes(name, 2))
        )
    return model


@pytest.fixture
def make_model():
    return _make_model


def _models_are_equal(a, b):
    print('### comparing', a.name, ':', a, 'to', b.name, ':', b)
    if type(a) != type(b):
        print('### fail on type', type(a), type(b))
        return False
    if a.name != b.name:
        print('### fail on name', a.name, b.name)
        return False
    if a.attributes != b.attributes:
        print('### fail on attributes', a.attributes, b.attributes)
        return False
    for a_child, b_child in zip(a.children(), b.children()):
        if not _models_are_equal(a_child, b_child):
            return False
    return True


@pytest.fixture
def models_are_equal():
    return _models_are_equal


# Dataset definition fixtures

def _make_dataset_defn(with_attributes=False, with_data=False):
    def maybe(attributes):
        return attributes if with_attributes else None

    return {
        'dataset_name':
        {
            'type': 'Dataset',
            'attributes': maybe(attributes('ds', 1)),
            'children': {
                'sequence_name': {
                    'type': 'Sequence',
                    'attributes': maybe(attributes('seq', 2)),
                    'children': {
                        'a': {
                            'type': 'String',
                            'attributes': maybe(attributes('a', 2)),
                        },
                        'b': {
                            'type': 'Float32',
                            'attributes': maybe(attributes('b', 2)),
                        },
                        'c': {
                            'type': 'Float32',
                            'attributes': maybe(attributes('c', 2)),
                        },
                    }
                }
            }
        }
    }


@pytest.fixture
def make_dataset_defn():
    return _make_dataset_defn


# Config file fixtures

@pytest.fixture
def config_file_1(make_dataset_defn):
    """
    Fixture returning the name of a temporary data file containing a dataset
    specification identical to that of _make_model(False).
    """
    with NamedTemporaryFile(mode='w', delete=False) as file:
        config = {
            'database': None,
            'dataset': make_dataset_defn(False),
        }
        file.write(yaml.dump(config, default_flow_style=False))

    yield file.name

    os.remove(file.name)


# Database fixtures

@pytest.fixture
def database(postgresql_db, database_uri):
    postgresql_db.create_table(Fruit)
    postgresql_db.session.add(Fruit(id=1, name='cherimoya'))
    postgresql_db.session.add(Fruit(id=2, name='durian'))
    postgresql_db.session.add(Fruit(id=3, name='medlar'))
    postgresql_db.session.commit()
    return {
        'session': postgresql_db.session,
        'uri': database_uri
    }

import pytest

from pydap.model import DatasetType, BaseType, SequenceType
from pydap.handlers.sqlalchemy import dds_spec_to_model

# TODO: Turn these into fixtures
def model1():
    model = DatasetType('dataset_name')
    model['sequence_name'] = SequenceType('sequence_name')
    for name in ['a', 'b', 'c']:
        model['sequence_name'][name] = BaseType(name)
    return model


def model2():
    model = DatasetType(name='dataset_name', attributes={
        'ds1': 'ds1_value'
    })
    model['sequence_name'] = SequenceType(name='sequence_name', attributes={
        'seq1': 'seq1_value',
        'seq2': 'seq2_value',
    })
    for name in ['a', 'b', 'c']:
        model['sequence_name'][name] = BaseType(name=name, attributes={
            '{}1'.format(name): '{}1_value'.format(name),
            '{}2'.format(name): '{}2_value'.format(name),
        })
    return model


def models_are_equal(a, b):
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
        if not models_are_equal(a_child, b_child):
            return False
    return True

@pytest.mark.parametrize('a, b, equal', [
    (model1(), model1(), True),
    (model1(), model2(), False),
    (model2(), model1(), False),
    (model2(), model2(), True),
])
def test_models_are_equal(a, b, equal):
    assert models_are_equal(a, b) == equal


@pytest.mark.parametrize('name, declaration, exp_model', [
    (
        'dataset_name',
        {
            'type': 'Dataset',
            'children': {
                'sequence_name': {
                    'type': 'Sequence',
                    'children': {
                        'a': 'String',
                        'b': 'Float32',
                        'c': 'Float32',
                    }
                }
            }
        },
        model1()
    ),
    (
        'dataset_name',
        {
            'type': 'Dataset',
            'attributes': {
                'ds1': 'ds1_value'
            },
            'children': {
                'sequence_name': {
                    'type': 'Sequence',
                    'attributes': {
                        'seq1': 'seq1_value',
                        'seq2': 'seq2_value',
                    },
                    'children': {
                        'a': {
                            'type': 'String',
                            'attributes': {
                                'a1': 'a1_value',
                                'a2': 'a2_value',
                            }
                        },
                        'b':  {
                            'type': 'Float32',
                            'attributes': {
                                'b1': 'b1_value',
                                'b2': 'b2_value',
                            }
                        },
                        'c':  {
                            'type': 'Float32',
                            'attributes': {
                                'c1': 'c1_value',
                                'c2': 'c2_value',
                            }
                        },
                    }
                }
            }
        },
        model2()
    ),
])
def test_dds_spec_to_model(name, declaration, exp_model):
    model = dds_spec_to_model(name, declaration, {})
    assert models_are_equal(model, exp_model)

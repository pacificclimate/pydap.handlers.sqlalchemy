import pytest

from pydap.model import DatasetType, BaseType, SequenceType
from pydap.handlers.sqlalchemy import dds_spec_to_model
from pydap.handlers.sqlalchemy import dict_merge

e = {}
d1 = { 'a': 1, 'b': 2 }
d2 = { 'a': 10, 'b': 20}
d3 = { 'a': 10, 'c': 3 }
d4 = { 'x': { 'a': 1, 'b': 2 }, 'y': { 'c': 3, 'd': 4 } }
d5 = { 'x': { 'a': 10, 'b': 2 }, 'y': { 'c': 3, 'd': 40 } }
d6 = { 'x': { 'a': 1, 'b': 2 }, 'y': { 'c': 3, 'd': 4 }, 'w': 8 }
d7 = { 'x': { 'a': 10, 'b': 2 }, 'y': { 'c': 3, 'd': 40 }, 'z': 9 }
d8 = { 'x': d1.copy() }
d9 = { 'x': d3.copy() }
@pytest.mark.parametrize('a, b, result', [
    (d1, e, d1),
    (e, d1, d1),
    (d1, d1, d1),
    (d1, d2, d2),
    (d2, d1, d1),
    (d1, d3, { 'a': 10, 'b': 2, 'c': 3 }),
    (d3, d1, { 'a': 1, 'b': 2, 'c': 3 }),
    (d4, d5, d5),
    (d5, d4, d4),
    (d6, d7, { 'x': { 'a': 10, 'b': 2 }, 'y': { 'c': 3, 'd': 40 }, 'z': 9, 'w': 8 }),
    (d7, d6, { 'x': { 'a': 1, 'b': 2 }, 'y': { 'c': 3, 'd': 4 }, 'z': 9, 'w': 8 }),
    (d8, d9, { 'x': { 'a': 10, 'b': 2, 'c': 3 } }),
    (d9, d8, { 'x': { 'a': 1, 'b': 2, 'c': 3 } }),
])
def test_dict_merge(a, b, result):
    assert dict_merge(a, b) == result


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


@pytest.mark.parametrize('spec, exp_model', [
    (
        {
            'Dataset': {
                'name': 'dataset_name',
                'items': [
                    {
                        'Sequence': {
                            'name': 'sequence_name',
                            'items': [
                                { 'String': 'a' },
                                { 'Float32': 'b' },
                                { 'Float32': 'c' },
                            ]
                        }
                    }
                ]
            }
        },
        model1()
    ),
    (
        {
            'Dataset': {
                'name': 'dataset_name',
                'attributes': {
                    'ds1': 'ds1_value'
                },
                'items': [
                    {
                        'Sequence': {
                            'name': 'sequence_name',
                            'attributes': {
                                'seq1': 'seq1_value',
                                'seq2': 'seq2_value',
                            },
                            'items': [
                                { 
                                    'String': {
                                        'name': 'a',
                                        'attributes': {
                                            'a1': 'a1_value',
                                            'a2': 'a2_value',
                                        }
                                    },
                                },
                                { 
                                    'Float32': {
                                        'name': 'b',
                                        'attributes': {
                                            'b1': 'b1_value',
                                            'b2': 'b2_value',
                                        }
                                    } 
                                },
                                { 
                                    'Float32': {
                                        'name': 'c',
                                        'attributes': {
                                            'c1': 'c1_value',
                                            'c2': 'c2_value',
                                        }
                                    } 
                                },
                            ]
                        }
                    }
                ]
            }
        },
        model2()
    ),
])
def test_dds_spec_to_model(spec, exp_model):
    model = dds_spec_to_model(spec)
    assert models_are_equal(model, exp_model)

"""
PyDAP SQLAlchemy handler

This handler allows PyDAP to serve data from any relational database
supported by SQLAlchemy.

Details TBD
"""
import re

from pydap.model import DatasetType, BaseType, SequenceType
from pydap.handlers.lib import BaseHandler
from pydap.exceptions import OpenFileError, ConstraintExpressionError


_dds_to_type = {
    'Dataset': DatasetType,
    'Sequence': SequenceType,
}
def dds_to_type(name):
    return _dds_to_type.get(name, BaseType)


def dds_spec_to_model(name, declaration):
    if not isinstance(declaration, (str, dict)):
        raise ValueError(
            'DDS declaration must be either a string or a dictionary.'
            'item {} of type {} is not valid'
                .format(name, type(declaration))
        )

    # Simple case: declaration is string specifying type (only)
    if type(declaration) is str:
        Type = dds_to_type(declaration)
        return Type(name=name)

    # General case: declaration is dict specifying type, attributes, children
    Type = dds_to_type(declaration['type'])
    # Construct parent model
    model = Type(
        name=name,
        attributes=declaration.get('attributes')
    )
    # Add children
    for child_name, child_declaration in declaration.get('children', {}).items():
        model[child_name] = dds_spec_to_model(child_name, child_declaration)

    return model


def dataset_model(spec):
    """
    Return the dataset model defined by this spec.

    :param spec: A Dataset spec.
        A Dataset spec is a dict containing exactly one key-value pair.
        The key is the name of the Dataset, and the value must have property
        'type' == 'Dataset'
    :return:
    """
    if len(spec) != 1:
        raise ValueError(
            'DDS spec must contain exactly 1 key-value pair; found {}: '
            'keys {}'
            .format(len(spec), spec.keys())
        )
    name, declaration = next(iter(spec.items()))
    if not isinstance(declaration, dict) or declaration.get('type') != 'Dataset':
        raise ValueError(
            'DDS spec must specify Dataset at top level'
        )
    return dds_spec_to_model(name, declaration)


def dict_merge(a, b):
    """Return a new dict which is a deep merge of a and b, b superseding a."""
    def merge_props(a, b, key):
        if key in b:
            if key in a:
                return dict_merge(a[key], b[key])
            return b[key]
        return a[key]
    if isinstance(a, dict) and isinstance(b, dict):
        # This was supposed to be "simpler" but it's not. It is however correct.
        # return {
        #     **{ key: dict_merge(a[key], b[key]) if key in a else b[key]
        #           for key in b},
        #     **{ key: a[key] for key in a if key not in b}
        # }
        return {
            key: merge_props(a, b, key)
            for key in set(a.keys()).union(set(b.keys()))
        }
    # When a and b are not both dicts, b supersedes a, so merge(a,b) == b
    # In some versions of dictionary merger, this type mismatch is labeled
    # a conflict and an exception is raised, but that's not important here.
    return b


class SQLAlchemyHandler(BaseHandler):

    extensions = re.compile(r'^.*\.sqla$', re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)

        try:
            with open(filepath, 'Ur') as fp:
                fp = open(filepath, 'Ur')
                config = yaml.load(fp)
        except Exception as exc:
            message = 'Unable to open file {filepath}: {exc}'.format(filepath=filepath, exc=exc)
            raise OpenFileError(message)

        self.config = config
        self.dataset = dataset_model(config['dataset'])

    def update(self, config, merge=False):
        # Merging dataset specs is not likely to get you what you want.
        # Maybe this should be a shallow merge, or a shallow merge on dataset
        # at least.
        self.config = dict_merge(self.config, config) if merge else config
        self.dataset = dataset_model(self.config['dataset'])







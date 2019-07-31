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


def dds_spec_to_model(spec):
    """
    Convert a DDS specification (in dictionary form) to a PyDAP model.

    :param spec: DDS specification, which is a dictionary containing exactly
        one key-value pair.
        The key is the name of the datatype (a base type or a constructor).
        When the datatype is a base type, the value is a string giving the
        name of this item.
        When the datatype is a constructor, the value is a dict with keys
        `name` (giving the name of this item), and `items` (giving the content
        of this item).
    """

    spec_content = spec.items()
    if len(spec_content) != 1:
        raise TypeError(
            'DDS spec must contain exactly 1 key-value pair; found {}: '
            'keys {}'
            .format(len(spec_content), spec_content.keys())
        )
    type_name, declaration = next(iter(spec_content))
    Type = dds_to_type(type_name)
    if type(declaration) is str:
        return Type(declaration)
    model = Type(
        name = declaration['name'],
        attributes=declaration.get('attributes')
    )
    for item in declaration.get('items', []):
        item_model = dds_spec_to_model(item)
        # Duplication of the model name like this seems to be standard in PyDAP.
        model[item_model.name] = item_model
    return model


def dataset_model(config):
    """
    Return the Dataset model defined in this config.
    For the user's convenience we do the work of pulling out the key 'Dataset'
    into the special 1-key dict required by `dds_spec_to_model`.

    :param config: A dict containing the key 'Dataset'. The dict containing
        just this key and its value is a DDS specification.
    :return:
    """
    return dds_spec_to_model({ 'Dataset': config['Dataset'] })


def dict_merge(a, b):
    """Return a new dict which is a deep merge of a and b, b superseding a."""
    def merge_props(a, b, key):
        if key in b:
            if key in a:
                return dict_merge(a[key], b[key])
            return b[key]
        return a[key]
    if isinstance(a, dict) and isinstance(b, dict):
        # This was supposed to be "simpler", but it's not. It is correct.
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
        self.dataset = dataset_model(config)

    def update(self, config, merge=False):
        self.config = dict_merge(self.config, config) if merge else config
        self.dataset = dataset_model(self.config)







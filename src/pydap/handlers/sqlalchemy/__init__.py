"""
PyDAP SQLAlchemy handler

This handler allows PyDAP to serve data from any relational database
supported by SQLAlchemy.

A relational database can't naturally or easily be treated as file.
However, PyDAP builds in the idea that the data source is a file.
A nice solution to this, pioneered by the handler pydap.handler.sql,
is to provide configuration for a database, query(ies), and resulting data
via the "data" file that the handler reads.

This approach is usable in some cases, but the database queries are necessarily
static because they read from a file. It's possible, but unpleasant, to get
around this by programmatically writing files that are then read by the handler.

A more elegant and flexible solution is to provide a method to update the
configuration directly from code.

Note that the configuration is fully declarative:
The builder of the configuration object does not need to know
how to build a PyDAP model nor how to wrangle the data sources into a form
consumable by PyDAP. The code in SQLAlchemyHandler and SQLAlchemyData do the
specialized grunt work of converting the configuration to all the necessary
derived objects.

A configuration object, whether read from a file or updated dynamically, has
the following structure (expressed in YAML)::

    database:
        dsn: <DSN>

    dataset:
        <dataset-spec>

where <DSN> is a database data source name, and <dataset-spec> is an (ordered)
dictionary defining the dataset, defined by the following extended BNF grammar.

Note: BNF (Backus-Naur form) is a notation for context-free grammars, i.e.,
for the syntax of specialized languages, like this one. A BNF specification
is a set of derivation rules, written as::

    <symbol> ::= __expression__

where :code:``<symbol>`` is a nonterminal (a metlinguistic variable), and
:code:``__expression__``, onsists of one or more sequences of symbols;
more sequences are separated by the vertical bar "|", indicating a choice,
the whole being a possible substitution for the symbol on the left.

As is common in BNF, we extend it with the use of regular expression operators
such as `*` and `+`, and with the use of square brackets to surround optional
items.

Here we also extend BNF by allowing YAML as part of the syntax of the
definition of an :code:``__expression__``.
YAML denotes in its usual way dictionaries, lists, and other data objects.
Therefore, unlike standard BNF, spacing and indentation matter in the
definition of :code:``__expression__``s.
::

    <dataset-spec> ::=
        <name>:
            type: Dataset
            [<attributes>]
            children:
                <DDS>+

    <DDS> ::=
        <name>: <data-type> |
        <name>:
            type: <data-type>
            [data: <data-source>]
            [<attributes>]
            [children:
                <DDS>+
            ]

    <attributes> ::=
        attributes:
            (<name>: <atomic-value>)+

    <data-type> ::= <constructor> | <base-type>

    <constructor> ::= Array | Structure | Sequence | Grid

    <base-type> ::= Byte | Int16 | UInt16 | Int32 | UInt32 | Float32 |
        Float64 | String | Url

Other nonterminals need a bit more explanation:

* `<name>` is any valid identifier you wish to assign to an item.

* `<data-source>` can be any of a number of valid sources of data, including:

  * A string of the form SQL{{<query>}}, specifying a database query.
  * A function, which is assumed to perform a database query and is therefore
    invoked with a database session as its argument. The return value is
    assumed to be an iterable of the query result rows.
  * An iterable.
  * An atomic value (including any string not matching the SQL query syntax
    above), which is used uninterpreted.


Example of a static dataset (in YAML)::

    station:
        type: Dataset
        attributes:
            station_name: FRASER
        children:
            observations:
                type: Sequence
                data: |
                    SQL{{
                    SELECT *
                    FROM observations NATURAL JOIN stations
                    WHERE station_name = 'FRASER'
                    }}
                children:
                    time: String
                    ONE_DAY_PRECIPITATION:
                        type: Float32
                        attributes:
                            units: mm
                    MAX_TEMP:
                        type: Float32
                        attributes:
                            units: degrees_C
                    MIN_TEMP:
                        type: Float32
                        attributes:
                            units: degrees_C




TODO:

* X Don't deep-merge configurations

* X Remove dict_merge

* X Figure out SQLAlchemyData < IterData args ifilter, imap, islice

* X Relatedly, figure out SQLAlchemyData args selection (same as ifilter?),
  slice (same as islice?) adopted from SQLData.
  * These are cruft from an earlier version of PyDAP. Superseded by ifilter,
    imap, islice.

* Write tests

* Complete this docstring. Talk about declarative approach.

"""
from contextlib import contextmanager
import re

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import yaml
import numpy as np

from pydap.model import DatasetType, BaseType, SequenceType
from pydap.handlers.lib import BaseHandler, IterData
from pydap.exceptions import OpenFileError, ConstraintExpressionError


_dds_to_type = {
    'Dataset': DatasetType,
    'Sequence': SequenceType,
}
def dds_to_type(name):
    return _dds_to_type.get(name, BaseType)


def dds_spec_to_model(name, declaration, db_config):
    """
    Convert a DDS spec to a PyDAP model, with data attached.

    A DDS spec is represented here by a name and declaration; this is
    the key-value pair extracted from a DDS spec in dict form.

    Recursively traverses the declaration's children.

    Adds data to the model element, derived from `declaration['data']`,
    if present.

    :param name: Name of the model element.
    :type name: str

    :param declaration: Declaration of the model element.
    :type declaration: str or dict

    :param db_config: Configuration for a database connection.
    :type db_config: dict

    :return:
    """
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
    for child_name, child_declaration in \
            declaration.get('children', {}).items():
        model[child_name] = \
            dds_spec_to_model(child_name, child_declaration, db_config)

    # Add data
    if 'data' in declaration:
        model.data = SQLAlchemyData(declaration['data'], db_config, model)

    return model


def dataset_model(config):
    """
    Return the dataset model defined in this configuration.

    :param config: A Dataset spec.
    :return:
    """

    # Extract and sanity check the dataset spec.
    spec = config['dataset']
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

    # Convert the specification to a model.
    return dds_spec_to_model(name, declaration, config['database'])


# Connection pool for database engines. Key is dsn for connection.
class EngineCreator(dict):
    def __missing__(self, key):
        self[key] = create_engine(key)
        return self[key]


Engines = EngineCreator()


# From http://docs.sqlalchemy.org/en/rel_0_9/orm/session.html#session-faq-whentocreate
@contextmanager
def session_scope(dsn):
    """
    Provide a transactional scope around a series of operations.
    Cleans up the connection even in the case of failure.
    """
    factory = sessionmaker(bind=Engines[dsn])
    session = factory()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class SQLAlchemyHandler(BaseHandler):

    extensions = re.compile(r'^.*\.sqla$', re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)

        if filepath is None:
            config = {}
        else:
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
        """
        Update the handler with a new or extended configuration.

        For details on the content of a configuration, see module docstring,
        above.

        :param config: Configuration.
        :type config: dict

        :param merge: If true, merge current and new configurations.
        :type merge: bool

        :return: self
        """

        self.config = {**self.config, **config} if merge else config
        self.dataset = dataset_model(self.config)


def stream(data_source, db_config):
    """
    Return an iterable that yields data items derived from a data source.

    :param data_source: Data source.
    :type data_source: Atomic value, iterable, or function.

    :param db_config: Configuration object for a database connection.
    :type db_config: dict

    If `data_source` is a function, it is assumed to be a query
    and is invoked with a SQLAlchemy session as the first argument.
    If `data_source` is a string of the form `SQL{{<content>}}`, then
    `<content>` is treated as an SQL query and is run against the database
    via SQLAlchemy.
    If `data_source` is an iterable, it is iterated.
    Otherwise, `data_source` is assumed to be an atomic value, and the
    resulting iterator yields just that one value.
    """

    # Convert the data source, which may not be directly usable,
    # into a usable data object.
    if callable(data_source):
        # A callable (function) is assumed to be a SQLAlchemy database
        # query, and is invoked with a SQLALchemy session.
        with session_scope(db_config['dsn']) as session:
            data = data_source(session)
    elif type(data_source) is str:
        # A string my be either a simple data item (one string)
        # or the text of a SQL query, delimited by `SQL{{<query>}}`.
        sql_re = re.compile(r'^\s*SQL\{\{(.*)\}\}\s*$')
        match = sql_re.search(data_source)
        if match is None:
            # It's just a string.
            data = data_source
        else:
            # It's a query. Run it.
            with session_scope(db_config['dsn']) as session:
                data = session.query(text(match.group(1)))
    else:
        data = data_source

    # Need to map/order query columns w.r.t. sequence (model_type) columns.
    # Or does IterData automagically do this? It looks like it might.

    # Return an iterator for the data.
    if hasattr(data, '__iter__'):
        return data
    else:
        def yield_one():
            yield data
        return yield_one()


class SQLAlchemyData(IterData):
    """
    This class converts a data source into an iterable data object
    suitable for consumption by a PyDAP model object.

    The data source can range from a simple atomic value to a function.
    If the latter, it is assumed the function performs a SQLAlchemy
    query, and so is invoked with a SQLAlchemy session.
    """
    def __init__(
            self, data_source, db_config, template,
            ifilter=None, imap=None, islice=None
    ):
        """
        Constructor.

        :param data_source: Data source.
        :type data_source: Atomic value, iterable, or function.

        :param db_config: Configuration object for a database connection.
        :type db_config: dict

        :param template: The (PyDAP) model for which the data source provides
            data.
        :type template: DapType

        :param ifilter:

        :param imap:

        :param islice:
        """
        self.data_source = data_source
        self.db_config = db_config
        super().__init__(
            stream=stream(data_source, db_config),
            template=template,
            ifilter=ifilter, imap=imap, islice=islice
        )

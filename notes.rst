=========================================================
Notes for a new SQL(Alchemy) handler
=========================================================

Goals
===========================

#. Config is a hash, not a file path
#. Result is a Sequence, as in existing SQL handler
#. Columns are defined more clearly
#. Query to generate sequence is passed in as a SQLAlchemy Query object

This is presented here in YAML format, but is to be received by :code:`__init__` as a hash.
It parallels the OPeNDAP DDS and DAS formats.

Retrieving station observations with SQLAlchemy
================================================

Original PDP code
-----------------------------------------------

Parameters:

* STATION_ID: int
* COMP: ['~' | '!~']

Variables per station
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    select
        vars_id,
        net_var_name,
        standard_name,
        cell_method
    from
        meta_vars natural join
        meta_station
    where
        station_id = STATION_ID
        AND cell_method COMP '(within|over)'
    order by net_var_name

Histories per station
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    select history_id
    from meta_history
    where station_id = STATION_ID

Observations per station
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    select
        obs_time
        -- for each station variable <VAR_NAME>:
        MAX(CASE WHEN vars_id =<VARS_ID> THEN datum END) as <VAR_NAME>
    from
        obs_raw
    where
        history_id in (<station history ids>)
        AND vars_id (<station variable ids>)
    group by obs_time
    order by obs_time

SQLAlchemy formulation using PyCDS
---------------------------------------------------------

::

    station_variables = (
        session.query(
            Variable.id,
            Variable.name,
        )
        .join(Station)
        .filter(Station.id == STATION_ID)
        .filter(Variable.cell_method.op(CMP)('(within|over)'))
        .all()
    )

    station_histories = (
        session.query(
            History.id
        )
        .filter(History.station_id == STATION_ID)
        .all()
    )

    station_observations = (
        session.query(
            Obs.time,
            *(
                func.max(case([
                    (Obs.vars_id == variable.id),
                ])).label(variable.name)
                for variable in station_variables
            )
        )
        .filter(Obs.history_id.in_(station_histories))
        .filter(Obs.vars_id.in_(station_variables))
        .group_by(Obs.time)
        .order_by(Obs.time)
        .all()
    )

This can probably be improved with joins on station histories and variables.

SQL formulation::

    select
        obs_time
        MAX(CASE WHEN vars_id =<VARS_ID> THEN datum END) as <VAR_NAME> -- for each station variable
    from
        obs_raw as obs
        join meta_vars as vars
        join meta_history as hx
    where
        obs.vars_id = vars.vars_id
        AND obs.history_id = hx.history_id
        AND hx.station_id = STATION_ID
        AND vars.cell_method COMP '(within|over)'
    group by obs_time
    order by obs_time

SQLAlchemy formuation::

    station_variables = (
        session.query(
            Variable.id,
            Variable.name,
        )
        .join(Station)
        .filter(Station.id == STATION_ID)
        .filter(Variable.cell_method.op(CMP)('(within|over)'))
        .all()
    )

    station_observations = (
        session.query(
            Obs.time,
            *(
                # Tricky shit to place the datum for each distinct variable into a distinct column.
                # Could this be done by gluing together columns from separate queries?
                func.max(case([
                    (Obs.vars_id == variable.id, Obs.datum), # Does this need an else clause?
                ])).label(variable.name)
                for variable in station_variables
            )
        )
        .join(Variable)
        .join(History)
        .filter(Obs.vars_id == Variable.id)
        .filter(Obs.history_id == History.id)
        .filter(History.station_id == STATION_ID)
        .filter(Variable.cell_method.op(CMP)('(within|over)'))
        .group_by(Obs.time)
        .order_by(Obs.time)
        .all()
    )

Alternative to tricky MAX(CASE(...)) shit:

::

    station_observations_by_variable = [
        session.query(
            Obs.time,
            Obs.datum.label(variable.name)
        )
        .join(Variable)
        .join(History)
        .filter(Obs.vars_id == variable.id)
        .filter(Obs.history_id == History.id)
        .filter(History.station_id == STATION_ID)
        .filter(Variable.cell_method.op(CMP)('(within|over)'))
        .group_by(Obs.time)
        .order_by(Obs.time)
        .all()
        for variable in station_variables
    ]

    # Then zip together the observations with matching times.
    # Result will be a row set with column labels that are the variable names.
    station_observations = zipmatch(station_observations_by_variable, ...)

This might be trickier (or much slower) than the tricky shit we are trying to avoid.

Later: It is certainly an interesting little problem to build `zipmatch`. I did it, but it
is fairly complex and probably not worth introducing unless the original tricky query
turns out to be very # slow, which does not appear to be the case given its historical use.


Configuration structure
=================================

Generic hash/YAML format for a DDS (data)
---------------------------------------------------------

General (covers full DDS)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This echoes the C-like syntax of the DDS: Type first, then name and contents (items).

Q: Is this worth it?
A: Yes

::

    Dataset:
        name: dataset_name
        items:
        - Sequence:
              name: sequence_name
              items:
              - Float32: name1
              - String: name2
              - String: name3

Equivalently, closer to DDS syntax but harder to read (like DDS syntax):
::

    Dataset:
        items:
        - Sequence:
              items:
              - Float32: name1
              - String: name2
              - String: name3
              name: sequence_name
        name: dataset_name


Alternative general
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is more in the style of a modern programming language. Name first, then type and contents.
Probably best not to do this, despite the appeal to those of us who hate C syntax.

::

    dataset_name:
        type: Dataset
        items:
        - sequence_name:
              type: Sequence
              items:
              - name1: Float32
              - name2: String
              - name3: String


Simplified (Sequence datatype only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

But provides no name for the Dataset or Sequence.

Where do the names for the Dataset and the Sequence come from?
In original SQL handler, from the config file: :code:`dataset.name`, :code:`sequence.name`.

::

    Dataset:
        column1: type1
        column2: type2
        ...

We end up with something like the original confusing SQL handler syntax.
Bleh.

Generic hash/YAML layout for DAS (attributes)
-----------------------------------------------

This is a simpler problem, and follows the DAS sytnax:

::

    Attributes:
        name1:
            attr11: value11
            attr12: value12
        name2:
            attr21: value21


Config file
===========================

Config file has 3 main sections.

* :code:`database`: Database access and configuration.
* :code:`Dataset`: DDS. At this point, it must contain exactly one Sequence.
* :code:`Attributes`: DAS. Straightforward.

::

    database:
        dsn: dsn

    Dataset:
        name: dataset_name
        items:
        - Sequence:
              name: seq_name
              items:
              - Float32: name1
              - String: name2
              - String: name3

    Attributes:
        NC_GLOBAL:
            foo: bar
        name1:
            attr11: value11
            attr12: value12
        name2:
            attr21: value21


We will restrict the content (:code:`.items`) of :code:`config.Dataset` to a single element of type Sequence.
However, this can easily be expanded if more is needed.

What if we wanted to stream all the stations, each with their observations?

::

        Dataset:
        name: stations
        items:
        - Sequence:
            name: station
            items:
            - String: <network_name>
            - String: <station_name>
            - Sequence:
                  name: observations
                  items:
                  - String: time
                  - Float32: ONE_DAY_PRECIPITATION
                  - Float32: MAX_TEMP
                  - Float32: MIN_TEMP


Unfortunately this can't work (at least not in any obvious, simple way) because
each station can have a different set of variables, meaning that the Sequence
:code:`observations` is different (and not knowable ahead of time) for each station in the
:code:`station` Sequence.

This looks too hard to be worth the trouble, and we are pretty much cast back on
the approach of the original PCIC handler, which is to zip up a separate file for
each station, rather than produce a single large file for all stations. Rats.

Handler architecture
======================

PyDAP handlers are invoked by
::

    wsgi_app = Handler(filepath)

where :code:`filepath` nominally identifies the file to be handled.

Databases aren't like files; a query mediates between the database as a whole
and the actual data read and served. This does not exist in ordinary file handlers.
(The PyDAP data slice-and-dice is on top of the marshalling of data from the database.)

We propose a SQLAlchemy handler constructed as follows:

* The handler returned is a base handler for the database.
  It is configured by the file found at :code:`filepath`.
* The base handler can function as an ordinary handler (i.e., it is derived from
  :code:`BaseHandler`). (TODO: Establish default behaviour for base handler.)
* The base handler contains a (class) constructor for a derived handler.
  This handler is *not* file-oriented; it is database query oriented.
* The derived handler is also PyDAP handler (i.e., it too is derived from BaseHandler).
* The constructor for the derived handler accepts a configuration
  object and a data object::

    base_handler = SQLAlchemyHandler(filepath)
    derived_handler = base_handler.DataHandler(data_source, config)

* The derived handler's configuration extends/overrides the base handler's
  configuration.
* The derived handler's :code:`data_source` argument provides the query(ies) that
  determine what data is delivered by the handler.
* Alternatively, perhaps simpler, add a config-with-data method to the base handler::

    handler = SQLAlchemyHandler(filepath).with_data(data_source, config)


Dataset component
----------------------------------------

To accommodate this architecture, we extend the Dataset component of the configuration
as follows::

    Dataset:
        name: station
        items:
        - Sequence:
              name: observations
              data: <data component name>  # optional; defaults to value of name
              items:
              - <BaseType>:
                    name: <name>
                    data: <data component name> # optional; defaults to value of name
              - <BaseType>: <name>  # shorthand for above with data component absent
              - Float32: ONE_DAY_PRECIPITATION
              - Float32: MAX_TEMP
              - Float32: MIN_TEMP

The :code:`data` properties in the configuration specify how data is obtained from the
:code:`data_source`.

In general, selection of data from the data source cacades according to the nesting
of the DDS elements.

Suppose there is a nested hierarchy of constructors/base types with :code:`data`
properties :code:`data1`, :code:`data2`, ..., :code:`dataN`. Then the data source
for the most deeply nested item is :code:`data_source[data1][data2]...[dataN]`.
Each data source is processed according to the rules for its type, as specified above.

* Dataset

  * The selected data item is either (??) the data source itself (no selection)
    or it is selected by :code:`data` property. Hmmm, consistency vs. convenience ...
    Probably best to choose consistency.
  * The selected data item is a dict.

* Sequence

  * The selected data item is an iterator, and each successive value returned
    by the iterator generates a single sequence output item.
  * Each output item
    is a named tuple or a dict from which items can be selected by the
    contents of the sequence (typically base types; see below; this corresponds
    to the notion of columns in the case of base types).

* Base type

  * The selected data item is a single atomic value, and it supplies the
    value for that item.

  * Type conversion?

Attributes
----------------------------------------------------------

**How will we handle Attributes? Similarly.**

Attributes are attached to the PyDAP model by supplying them as a parameter to the model
constructors.
Attributes are managed similarly to those in a NetCDF file.
Each item (this term needs a precise definition) can have any number of attributes associated
with it.
Items are identified ... HOW??? By name, but the convention for DDS allows repeated use of a
single name.

Alternatively, as done in pydap.handlers.sql, we could attach attributes in the DDS spec.
Example::

    Dataset:
        name: station
        attributes:
            attr1: value1
            attr2: value2
        items:
        - Sequence:
              name: observations
              attributes:
                  attr1: value1
                  attr2: value2
              data: <data component name>  # optional; defaults to value of name
              items:
              - <BaseType>:
                    name: <name>
                    attributes:
                        attr1: value1
                        attr2: value2
                    data: <data component name> # optional; defaults to value of name
              - <BaseType>: <name>  # shorthand for above with data and attributes components absent
              - Float32: ONE_DAY_PRECIPITATION
              - Float32: MAX_TEMP
              - Float32: MIN_TEMP


Integration/usage
===========================================================

Use case for station observations
------------------------------------------------------------

::

    base_handler = SQLAlchemyHandler('base_config.sqla')

    station_variables = (
        session.query(
            Variable.id,
            Variable.name,
        )
        .join(Station)
        .filter(Station.id == STATION_ID)
        .filter(Variable.cell_method.op(CMP)('(within|over)'))
        .all()
    )

    station_observations = (
        session.query(
            Obs.time,
            *(
                # Tricky shit to place the datum for each distinct variable into a distinct column.
                func.max(case([
                    (Obs.vars_id == variable.id, Obs.datum), # Does this need an else clause?
                ])).label(variable.name)
                for variable in station_variables
            )
        )
        .join(Variable)
        .join(History)
        .filter(Obs.vars_id == Variable.id)
        .filter(Obs.history_id == History.id)
        .filter(History.station_id == STATION_ID)
        .filter(Variable.cell_method.op(CMP)('(within|over)'))
        .group_by(Obs.time)
        .order_by(Obs.time)
    )

    config = {
        'Dataset':
            'name': 'station',
            'attributes': {
                'version': '1.0',
                'contact': rglover@uvic.ca
            },
            'items': [
                {
                    'Sequence': {
                        'name': 'observations',
                        'attributes': {
                            'count': station_observations.count()
                        },
                        'items': [
                            {
                                TYPE(variable): {                       # TODO
                                    'name': variable.name,
                                    'attributes': ATTRIBUTES(variable)  # TODO
                                }
                            }
                            for variable in station_variables
                        ]
                    }
                }
            ]
        },
 }

    data_source = {
        'station': {
            'observations': station_observations.all()
        }
    }

    derived_handler = base_handler.DataHandler(config, data_source)


Ooh, that looks pretty sexy.

Content of :code:`'base_config.sqla'`::

    database:
        dsn: ...

Simplification!!
=======================

Several things will make this a lot simpler. The main observations driving this are:

* When passing in a new configuration, there is no need to separate data source
  from config. Instead, each element of the DDS can specify a data source using the
  :code:`data` key.

* Data sources in the initial config read in from a file can be static (fixed literal values)
  or specified by a SQL expression. We can add a special syntax
  (e.g., :code:`${<sql statement>}` or :code:`SQL{<sql statement>}` or :code:`{{<sql statement>}}`,
  really just according to taste). This is like the original SQL handler.

* The data sources in the passed-in config can be dynamic; that is they can be one of the following:

  * Any dynamically generated Python object (e.g., a string, a list, an interator)
  * A function yielding data. This would be invoked with a SQLAlchemy session which would enable
    the function to perform a query on the database.

* The base handler / derived handler architecture is much more complicated than needed. Instead,
  add an :code:`update(config)` method that allows the configuration (and therefore dataset, data
  source, etc.) to be updated with a programmatically derived value.

  New typical usage becomes::

    handler = pydap.handlers.SQLAlchemyHandler(filepath).update(config)

* The DDS-like structure of the dict/YAML is awkward for a couple of reasons
  (although it is superficially, i.e., visually more similar to DDS syntax).
  It should be replaced by a structure that is directly congruent with the
  semantics of the declaration. Specifically it should use dicts, keyed by
  item names for all declarations, and avoid arrays where they are not a direct
  reflection of the semantics (which is almost nowhere).

  Example (in YAML)::

    datset_name:
        type: Dataset
        attributes:
            attr1: value1
        children:
            sequence_name:
                type: Sequence
                attributes:
                    attr2: value2
                children:
                    a:
                        type: String
                    b: String  # Shorthand for explicit type, no other props
                    c:
                        type: String
                        attributes:
                            attr3: value3

  Note: The above syntax depends on the order of keys being preserved.
  This is not a property of YAML mappings (hashes, dicts); they are
  explicitly unordered. However, the linear nature of a YAML file means
  that a natural insertion order is present and is almost
  certain to happen in any sane implementation.
  Python 3.7 declaration that dicts now (must) preserve insertion order.
  For Python <3.7 PyYAML can easily be invoked with an
  `OrderedDict loader <https://stackoverflow.com/a/21912744>`_.
  Given all that, we'll go with PyYAML and add the OrderedDict loader
  if necessary.

* Otherwise things remain much the same as above.

Data
=================================

Data is tough to understand deeply in PyDAP.

The following model type classes accept data:

* BaseType

* SequenceType
  * This type has data attached directly to it.
    * The data is an iterable (e.g., numpy array)
    * Each iteration of the data provides data for populating a row of the SequenceType.
  * The children of a SequenceType are BaseTypes.
    * In this role, they do not have data attached directly to them.
    * Do they have data type? It appears from the PyDAP model module docstring that the datatype
      of a child is derived from the datatype of elements of the parent rows.
      Need to understand numpy arrays. Need to understand numpy data types.
      That example seems to show that we need to convert the output of the database query to
      a numpy array with datatypes attached that contain both the column (child) name and the
      numpy type.


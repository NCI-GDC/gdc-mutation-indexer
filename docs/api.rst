GDC Mutation Indexer API
========================

Configuration
#############

.. automodule:: config
   :members:


Builders
########

Builders contain the central logic of index creation.

.. automodule:: exports.builders
   :members:

.. automodule:: exports.builders.base_builder
   :members:

.. automodule:: exports.builders.case_centric
   :members:

.. automodule:: exports.builders.gene_centric
   :members:

.. automodule:: exports.builders.ssm_centric
   :members:

.. automodule:: exports.builders.ssm_occurrence_centric
   :members:


Mappings
########

Mappings are yaml files that define the structure of the desired

Mappers
#######

Mappers convert Mappings into Elasticsearch index mappings

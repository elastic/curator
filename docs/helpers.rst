.. _helpers:

Helpers
#######

.. _helpers_date_ops:

Date Ops
========

.. py:module:: curator.helpers.date_ops

.. autoclass:: TimestringSearch
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: absolute_date_range

.. autofunction:: date_range

.. autofunction:: datetime_to_epoch

.. autofunction:: fix_epoch

.. autofunction:: get_date_regex

.. autofunction:: get_datemath

.. autofunction:: get_datetime

.. autofunction:: get_point_of_reference

.. autofunction:: get_unit_count_from_name

.. autofunction:: handle_iso_week_number

.. autofunction:: isdatemath

.. autofunction:: parse_date_pattern

.. autofunction:: parse_datemath

.. _helpers_getters:

Getters
=======

.. py:module:: curator.helpers.getters

.. autofunction:: byte_size

.. autofunction:: get_indices

.. autofunction:: get_repository

.. autofunction:: get_snapshot

.. autofunction:: get_snapshot_data

.. autofunction:: get_write_index

.. autofunction:: index_size

.. autofunction:: name_to_node_id

.. autofunction:: node_id_to_name

.. autofunction:: node_roles

.. autofunction:: single_data_path

.. _helpers_testers:

Testers
=======

.. py:module:: curator.helpers.testers

.. autofunction:: repository_exists

.. autofunction:: rollable_alias

.. autofunction:: snapshot_running

.. autofunction:: validate_actions

.. autofunction:: validate_filters

.. autofunction:: verify_client_object

.. autofunction:: verify_repository

.. autofunction:: verify_snapshot_list
.. _helpers_utils:

Utils
=====

.. py:module:: curator.helpers.utils

.. autofunction:: chunk_index_list

.. autofunction:: report_failure

.. autofunction:: show_dry_run

.. autofunction:: to_csv

.. _helpers_waiters:

Waiters
=======

.. py:module:: curator.helpers.waiters

.. autofunction:: health_check

.. autofunction:: relocate_check

.. autofunction:: restore_check

.. autofunction:: snapshot_check

.. autofunction:: task_check

.. autofunction:: wait_for_it

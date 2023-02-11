from elasticsearch8 import ConflictError, NotFoundError, TransportError

fake_fail      = Exception('Simulated Failure')
four_oh_one    = TransportError(401, "simulated error")
four_oh_four   = TransportError(404, "simulated error")
get_alias_fail = NotFoundError(404, 'simulated error', 'simulated error')
named_index    = 'index_name'
named_indices  = [ "index-2015.01.01", "index-2015.02.01" ]
open_index     = {'metadata': {'indices' : { named_index : {'state' : 'open'}}}}
closed_index   = {'metadata': {'indices' : { named_index : {'state' : 'close'}}}}
cat_open_index = [{'status': 'open'}]
cat_closed_index = [{'status': 'close'}]
open_indices   = { 'metadata': { 'indices' : { 'index1' : { 'state' : 'open' },
                                               'index2' : { 'state' : 'open' }}}}
closed_indices = { 'metadata': { 'indices' : { 'index1' : { 'state' : 'close' },
                                               'index2' : { 'state' : 'close' }}}}
named_alias    = 'alias_name'
alias_retval   = { "pre_aliased_index": { "aliases" : { named_alias : { }}}}
rollable_alias = { "index-000001": { "aliases" : { named_alias : { }}}}
rollover_conditions = { 'conditions': { 'max_age': '1s' } }
dry_run_rollover = {
  "acknowledged": True,
  "shards_acknowledged": True,
  "old_index": "index-000001",
  "new_index": "index-000002",
  "rolled_over": False,
  "dry_run": True,
  "conditions": {
    "max_age" : "1s"
  }
}
aliases_retval = {
    "index1": { "aliases" : { named_alias : { } } },
    "index2": { "aliases" : { named_alias : { } } },
    }
alias_one_add  = [{'add': {'alias': 'alias', 'index': 'index_name'}}]
alias_one_add_with_extras  = [
    { 'add': {
            'alias': 'alias', 'index': 'index_name',
            'filter' : { 'term' : { 'user' : 'kimchy' }}
            }
    }]
alias_one_rm   = [{'remove': {'alias': 'my_alias', 'index': named_index}}]
alias_one_body = { "actions" : [
                        {'remove': {'alias': 'alias', 'index': 'index_name'}},
                        {'add': {'alias': 'alias', 'index': 'index_name'}}
                 ]}
alias_two_add  = [
                    {'add': {'alias': 'alias', 'index': 'index-2016.03.03'}},
                    {'add': {'alias': 'alias', 'index': 'index-2016.03.04'}},
                 ]
alias_two_rm   = [
                    {'remove': {'alias': 'my_alias', 'index': 'index-2016.03.03'}},
                    {'remove': {'alias': 'my_alias', 'index': 'index-2016.03.04'}},
                 ]
alias_success  = { "acknowledged": True }
allocation_in  = {named_index: {'settings': {'index': {'routing': {'allocation': {'require': {'foo': 'bar'}}}}}}}
allocation_out = {named_index: {'settings': {'index': {'routing': {'allocation': {'require': {'not': 'foo'}}}}}}}
indices_space  = { 'indices' : {
        'index1' : { 'index' : { 'primary_size_in_bytes': 1083741824 }},
        'index2' : { 'index' : { 'primary_size_in_bytes': 1083741824 }}}}
snap_name      = 'snap_name'
repo_name      = 'repo_name'
test_repo      = {repo_name: {'type': 'fs', 'settings': {'compress': 'true', 'location': '/tmp/repos/repo_name'}}}
test_repos     = {'TESTING': {'type': 'fs', 'settings': {'compress': 'true', 'location': '/tmp/repos/TESTING'}},
                  repo_name: {'type': 'fs', 'settings': {'compress': 'true', 'location': '/rmp/repos/repo_name'}}}
snap_running   = { 'snapshots': ['running'] }
nosnap_running = { 'snapshots': [] }
snapshot       = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-02-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'SUCCESS',
                        'snapshot': snap_name, 'end_time': '2015-02-01T00:00:01.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1422748800
                    }]}
oneinprogress  = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-03-01T00:00:02.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'IN_PROGRESS',
                        'snapshot': snap_name, 'end_time': '2015-03-01T00:00:03.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1425168002
                    }]}
partial        = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-02-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'PARTIAL',
                        'snapshot': snap_name, 'end_time': '2015-02-01T00:00:01.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1422748800
                    }]}
failed         = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-02-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'FAILED',
                        'snapshot': snap_name, 'end_time': '2015-02-01T00:00:01.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1422748800
                    }]}
othersnap      = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-02-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'SOMETHINGELSE',
                        'snapshot': snap_name, 'end_time': '2015-02-01T00:00:01.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1422748800
                    }]}
snapshots         = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-02-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'SUCCESS',
                        'snapshot': snap_name, 'end_time': '2015-02-01T00:00:01.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1422748800
                    },
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-03-01T00:00:02.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'SUCCESS',
                        'snapshot': 'snapshot-2015.03.01', 'end_time': '2015-03-01T00:00:03.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1425168002
                    }]}
inprogress        = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-02-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'SUCCESS',
                        'snapshot': snap_name, 'end_time': '2015-02-01T00:00:01.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1422748800
                    },
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-03-01T00:00:02.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'IN_PROGRESS',
                        'snapshot': 'snapshot-2015.03.01', 'end_time': '2015-03-01T00:00:03.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1425168002
                    }]}
highly_unlikely   = { 'snapshots': [
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-02-01T00:00:00.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'IN_PROGRESS',
                        'snapshot': snap_name, 'end_time': '2015-02-01T00:00:01.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1422748800
                    },
                    {
                        'duration_in_millis': 60000, 'start_time': '2015-03-01T00:00:02.000Z',
                        'shards': {'successful': 4, 'failed': 0, 'total': 4},
                        'end_time_in_millis': 0, 'state': 'IN_PROGRESS',
                        'snapshot': 'snapshot-2015.03.01', 'end_time': '2015-03-01T00:00:03.000Z',
                        'indices': named_indices,
                        'failures': [], 'start_time_in_millis': 1425168002
                    }]}
snap_body_all   = {
                    "ignore_unavailable": False,
                    "include_global_state": True,
                    "partial": False,
                    "indices" : "_all"
                  }
snap_body       = {
                    "ignore_unavailable": False,
                    "include_global_state": True,
                    "partial": False,
                    "indices" : "index-2015.01.01,index-2015.02.01"
                  }
verified_nodes  = {'nodes': {'nodeid1': {'name': 'node1'}, 'nodeid2': {'name': 'node2'}}}
synced_pass     = {
                    "_shards":{"total":1,"successful":1,"failed":0},
                    "index_name":{
                        "total":1,"successful":1,"failed":0,
                        "failures":[],
                    }
                  }
synced_fail     = {
                    "_shards":{"total":1,"successful":0,"failed":1},
                    "index_name":{
                        "total":1,"successful":0,"failed":1,
                        "failures":[
                            {"shard":0,"reason":"pending operations","routing":{"state":"STARTED","primary":True,"node":"nodeid1","relocating_node":None,"shard":0,"index":"index_name"}},
                        ]
                    }
                  }
sync_conflict   = ConflictError(409, '{"_shards":{"total":1,"successful":0,"failed":1},"index_name":{"total":1,"successful":0,"failed":1,"failures":[{"shard":0,"reason":"pending operations","routing":{"state":"STARTED","primary":true,"node":"nodeid1","relocating_node":null,"shard":0,"index":"index_name"}}]}})', synced_fail)
synced_fails    = {
                    "_shards":{"total":2,"successful":1,"failed":1},
                    "index1":{
                        "total":1,"successful":0,"failed":1,
                        "failures":[
                            {"shard":0,"reason":"pending operations","routing":{"state":"STARTED","primary":True,"node":"nodeid1","relocating_node":None,"shard":0,"index":"index_name"}},
                        ]
                    },
                    "index2":{
                        "total":1,"successful":1,"failed":0,
                        "failures":[]
                    },
                  }

settings_one   = {
    named_index: {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'random_uuid_string_here',
                'number_of_shards': '2', 'creation_date': '1456963200172',
                'routing': {'allocation': {'include': {'tag': 'foo'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    }
}

settings_1_get_aliases = { named_index: { "aliases" : { 'my_alias' : { } } } }

settings_two  = {
    'index-2016.03.03': {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'random_uuid_string_here',
                'number_of_shards': '5', 'creation_date': '1456963200172',
                'routing': {'allocation': {'include': {'tag': 'foo'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    },
    'index-2016.03.04': {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'another_random_uuid_string',
                'number_of_shards': '5', 'creation_date': '1457049600812',
                'routing': {'allocation': {'include': {'tag': 'bar'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    }
}

settings_2_get_aliases = {
    "index-2016.03.03": { "aliases" : { 'my_alias' : { } } },
    "index-2016.03.04": { "aliases" : { 'my_alias' : { } } },
}

settings_2_closed = {
    'index-2016.03.03': {
        'state': 'close',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'random_uuid_string_here',
                'number_of_shards': '5', 'creation_date': '1456963200172',
                'routing': {'allocation': {'include': {'tag': 'foo'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    },
    'index-2016.03.04': {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'another_random_uuid_string',
                'number_of_shards': '5', 'creation_date': '1457049600812',
                'routing': {'allocation': {'include': {'tag': 'bar'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    }
}

settings_two_no_cd  = {
    'index-2016.03.03': {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'random_uuid_string_here',
                'number_of_shards': '5', 'creation_date': '1456963200172',
                'routing': {'allocation': {'include': {'tag': 'foo'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    },
    'index-2016.03.04': {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'another_random_uuid_string',
                'number_of_shards': '5',
                'routing': {'allocation': {'include': {'tag': 'bar'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    }
}

settings_four  = {
    'a-2016.03.03': {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'random_uuid_string_here',
                'number_of_shards': '5', 'creation_date': '1456963200172',
                'routing': {'allocation': {'include': {'tag': 'foo'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    },
    'b-2016.03.04': {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'another_random_uuid_string',
                'number_of_shards': '5', 'creation_date': '1457049600812',
                'routing': {'allocation': {'include': {'tag': 'bar'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    },
    'c-2016.03.05': {
        'state': 'close',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'random_uuid_string_here',
                'number_of_shards': '5', 'creation_date': '1457136000933',
                'routing': {'allocation': {'include': {'tag': 'foo'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    },
    'd-2016.03.06': {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'another_random_uuid_string',
                'number_of_shards': '5', 'creation_date': '1457222400527',
                'routing': {'allocation': {'include': {'tag': 'bar'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    }
}

settings_named = {
    'index-2015.01.01': {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'random_uuid_string_here',
                'number_of_shards': '5', 'creation_date': '1456963200172',
                'routing': {'allocation': {'include': {'tag': 'foo'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    },
    'index-2015.02.01': {
        'state': 'open',
        'aliases': ['my_alias'],
        'mappings': {},
        'settings': {
            'index': {
                'number_of_replicas': '1', 'uuid': 'another_random_uuid_string',
                'number_of_shards': '5', 'creation_date': '1457049600812',
                'routing': {'allocation': {'include': {'tag': 'bar'}}},
                'version': {'created': '2020099'}, 'refresh_interval': '5s'
            }
        }
    }
}

clu_state_one  = {
    'metadata': {
        'indices': settings_one
    }
}
clu_state_two  = {
    'metadata': {
        'indices': settings_two
    }
}
cs_two_closed  = {
    'metadata': {
        'indices': settings_2_closed
    }
}
clu_state_two_no_cd  = {
    'metadata': {
        'indices': settings_two_no_cd
    }
}
clu_state_four = {
    'metadata': {
        'indices': settings_four
    }
}

stats_one      = {
    'indices': {
        named_index : {
            'total': {
                'docs': {'count': 6374962, 'deleted': 0},
                'store': {'size_in_bytes': 1115219663, 'throttle_time_in_millis': 0}
            },
            'primaries': {
                'docs': {'count': 3187481, 'deleted': 0},
                'store': {'size_in_bytes': 557951789, 'throttle_time_in_millis': 0}
            }
        }
    }
}

stats_two      = {
    'indices': {
        'index-2016.03.03': {
            'total': {
                'docs': {'count': 6374962, 'deleted': 0},
                'store': {'size_in_bytes': 1115219663, 'throttle_time_in_millis': 0}
            },
            'primaries': {
                'docs': {'count': 3187481, 'deleted': 0},
                'store': {'size_in_bytes': 557951789, 'throttle_time_in_millis': 0}
            }
        },
        'index-2016.03.04': {
            'total': {
                'docs': {'count': 6377544, 'deleted': 0},
                'store': {'size_in_bytes': 1120891046, 'throttle_time_in_millis': 0}
            },
            'primaries': {
                'docs': {'count': 3188772, 'deleted': 0},
                'store': {'size_in_bytes': 560677114, 'throttle_time_in_millis': 0}
            }
        }
    }
}

stats_four      = {
    'indices': {
        'a-2016.03.03': {
            'total': {
                'docs': {'count': 6374962, 'deleted': 0},
                'store': {'size_in_bytes': 1115219663, 'throttle_time_in_millis': 0}
            },
            'primaries': {
                'docs': {'count': 3187481, 'deleted': 0},
                'store': {'size_in_bytes': 557951789, 'throttle_time_in_millis': 0}
            }
        },
        'b-2016.03.04': {
            'total': {
                'docs': {'count': 6377544, 'deleted': 0},
                'store': {'size_in_bytes': 1120891046, 'throttle_time_in_millis': 0}
            },
            'primaries': {
                'docs': {'count': 3188772, 'deleted': 0},
                'store': {'size_in_bytes': 560677114, 'throttle_time_in_millis': 0}
            }
        },
        # CLOSED, ergo, not present
        # 'c-2016.03.05': {
        #     'total': {
        #         'docs': {'count': 6266434, 'deleted': 0},
        #         'store': {'size_in_bytes': 1120882166, 'throttle_time_in_millis': 0}
        #     },
        #     'primaries': {
        #         'docs': {'count': 3133217, 'deleted': 0},
        #         'store': {'size_in_bytes': 560441083, 'throttle_time_in_millis': 0}
        #     }
        # },
        'd-2016.03.06': {
            'total': {
                'docs': {'count': 6266436, 'deleted': 0},
                'store': {'size_in_bytes': 1120882168, 'throttle_time_in_millis': 0}
            },
            'primaries': {
                'docs': {'count': 3133218, 'deleted': 0},
                'store': {'size_in_bytes': 560441084, 'throttle_time_in_millis': 0}
            }
        }

    }
}

fieldstats_one = {
    'indices': {
        named_index : {
            'fields': {
                'timestamp': {
                    'density': 100,
                    'min_value_as_string': '2016-03-03T00:00:06.189Z',
                    'max_value': 1457049599152, 'max_doc': 415651,
                    'min_value': 1456963206189, 'doc_count': 415651,
                    'max_value_as_string': '2016-03-03T23:59:59.152Z',
                    'sum_total_term_freq': -1, 'sum_doc_freq': 1662604}}}}
    }

fieldstats_two = {
    'indices': {
        'index-2016.03.03': {
            'fields': {
                'timestamp': {
                    'density': 100,
                    'min_value_as_string': '2016-03-03T00:00:06.189Z',
                    'max_value': 1457049599152, 'max_doc': 415651,
                    'min_value': 1456963206189, 'doc_count': 415651,
                    'max_value_as_string': '2016-03-03T23:59:59.152Z',
                    'sum_total_term_freq': -1, 'sum_doc_freq': 1662604}}},
        'index-2016.03.04': {
            'fields': {
                'timestamp': {
                    'density': 100,
                    'min_value_as_string': '2016-03-04T00:00:00.812Z',
                    'max_value': 1457135999223, 'max_doc': 426762,
                    'min_value': 1457049600812, 'doc_count': 426762,
                    'max_value_as_string': '2016-03-04T23:59:59.223Z',
                    'sum_total_term_freq': -1, 'sum_doc_freq': 1673715}}},
    }
}

fieldstats_four = {
    'indices': {
        'a-2016.03.03': {
            'fields': {
                'timestamp': {
                    'density': 100,
                    'min_value_as_string': '2016-03-03T00:00:06.189Z',
                    'max_value': 1457049599152, 'max_doc': 415651,
                    'min_value': 1456963206189, 'doc_count': 415651,
                    'max_value_as_string': '2016-03-03T23:59:59.152Z',
                    'sum_total_term_freq': -1, 'sum_doc_freq': 1662604}}},
        'b-2016.03.04': {
            'fields': {
                'timestamp': {
                    'density': 100,
                    'min_value_as_string': '2016-03-04T00:00:00.812Z',
                    'max_value': 1457135999223, 'max_doc': 426762,
                    'min_value': 1457049600812, 'doc_count': 426762,
                    'max_value_as_string': '2016-03-04T23:59:59.223Z',
                    'sum_total_term_freq': -1, 'sum_doc_freq': 1673715}}},
        'd-2016.03.06': {
            'fields': {
                'timestamp': {
                    'density': 100,
                    'min_value_as_string': '2016-03-04T00:00:00.812Z',
                    'max_value': 1457308799223, 'max_doc': 426762,
                    'min_value': 1457222400567, 'doc_count': 426762,
                    'max_value_as_string': '2016-03-04T23:59:59.223Z',
                    'sum_total_term_freq': -1, 'sum_doc_freq': 1673715}}},
    }
}

fieldstats_query = {
    'aggregations': {
        'min' : {
            'value_as_string': '2016-03-03T00:00:06.189Z',
            'value': 1456963206189,
        },
        'max' : {
            'value': 1457049599152,
            'value_as_string': '2016-03-03T23:59:59.152Z',
        }
    }
}

shards         = { 'indices': { named_index: { 'shards': {
        '0': [ { 'num_search_segments' : 15 }, { 'num_search_segments' : 21 } ],
        '1': [ { 'num_search_segments' : 19 }, { 'num_search_segments' : 16 } ] }}}}
fm_shards      = { 'indices': { named_index: { 'shards': {
        '0': [ { 'num_search_segments' : 1 }, { 'num_search_segments' : 1 } ],
        '1': [ { 'num_search_segments' : 1 }, { 'num_search_segments' : 1 } ] }}}}

loginfo        =    {   "loglevel": "INFO",
                        "logfile": None,
                        "logformat": "default"
                    }
default_format = '%(asctime)s %(levelname)-9s %(message)s'
debug_format   = '%(asctime)s %(levelname)-9s %(name)22s %(funcName)22s:%(lineno)-4d %(message)s'

yamlconfig     = '''
---
# Remember, leave a key empty to use the default value.  None will be a string,
# not a Python "NoneType"
client:
  hosts: http://127.0.0.1:9200
  certificate:
  client_cert:
  client_key:
  http_auth:
  timeout: 30
  master_only: False

options:
  dry_run: False
  loglevel: DEBUG
  logfile:
  logformat: default
  quiet: False
'''
pattern_ft     = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: pattern
        kind: prefix
        value: a
        exclude: False
'''
age_ft         = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: age
        source: name
        direction: older
        timestring: '%Y.%m.%d'
        unit: seconds
        unit_count: 0
        epoch: 1456963201
'''
space_ft         = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: space
        disk_space: 2.1
        source: name
        use_age: True
        timestring: '%Y.%m.%d'
'''
forcemerge_ft  = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: forcemerged
        max_num_segments: 2
'''
allocated_ft   = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: allocated
        key: tag
        value: foo
        allocation_type: include
'''
kibana_ft      = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: kibana
'''
opened_ft      = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: opened
'''
closed_ft     = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: closed
'''
none_ft        = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: none
'''
invalid_ft     = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: sir_not_appearing_in_this_film
'''
snap_age_ft    = '''
---
actions:
  1:
    description: test
    action: delete_snapshots
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: age
        direction: older
        unit: days
        unit_count: 1
'''
snap_pattern_ft= '''
---
actions:
  1:
    description: test
    action: delete_snapshots
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: pattern
        kind: prefix
        value: sna
'''
snap_none_ft  = '''
---
actions:
  1:
    description: test
    action: delete_snapshots
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: none
'''
size_ft     = '''
---
actions:
  1:
    description: open all matching indices
    action: open
    options:
      continue_if_exception: False
      disable_action: False
    filters:
      - filtertype: size
        size_threshold: 1.04
        size_behavior: total
        threshold_behavior: less_than
'''

generic_task = {'task': 'I0ekFjMhSPCQz7FUs1zJOg:54510686'}
incomplete_task = {'completed': False, 'task': {'node': 'I0ekFjMhSPCQz7FUs1zJOg', 'status': {'retries': {'bulk': 0, 'search': 0}, 'updated': 0, 'batches': 3647, 'throttled_until_millis': 0, 'throttled_millis': 0, 'noops': 0, 'created': 3646581, 'deleted': 0, 'requests_per_second': -1.0, 'version_conflicts': 0, 'total': 3646581}, 'description': 'UNIT TEST', 'running_time_in_nanos': 1637039537721, 'cancellable': True, 'action': 'indices:data/write/reindex', 'type': 'transport', 'id': 54510686, 'start_time_in_millis': 1489695981997}, 'response': {'retries': {'bulk': 0, 'search': 0}, 'updated': 0, 'batches': 3647, 'throttled_until_millis': 0, 'throttled_millis': 0, 'noops': 0, 'created': 3646581, 'deleted': 0, 'took': 1636917, 'requests_per_second': -1.0, 'timed_out': False, 'failures': [], 'version_conflicts': 0, 'total': 3646581}}
completed_task = {'completed': True, 'task': {'node': 'I0ekFjMhSPCQz7FUs1zJOg', 'status': {'retries': {'bulk': 0, 'search': 0}, 'updated': 0, 'batches': 3647, 'throttled_until_millis': 0, 'throttled_millis': 0, 'noops': 0, 'created': 3646581, 'deleted': 0, 'requests_per_second': -1.0, 'version_conflicts': 0, 'total': 3646581}, 'description': 'UNIT TEST', 'running_time_in_nanos': 1637039537721, 'cancellable': True, 'action': 'indices:data/write/reindex', 'type': 'transport', 'id': 54510686, 'start_time_in_millis': 1489695981997}, 'response': {'retries': {'bulk': 0, 'search': 0}, 'updated': 0, 'batches': 3647, 'throttled_until_millis': 0, 'throttled_millis': 0, 'noops': 0, 'created': 3646581, 'deleted': 0, 'took': 1636917, 'requests_per_second': -1.0, 'timed_out': False, 'failures': [], 'version_conflicts': 0, 'total': 3646581}}
completed_task_zero_total = {'completed': True, 'task': {'node': 'I0ekFjMhSPCQz7FUs1zJOg', 'status': {'retries': {'bulk': 0, 'search': 0}, 'updated': 0, 'batches': 0, 'throttled_until_millis': 0, 'throttled_millis': 0, 'noops': 0, 'created': 0, 'deleted': 0, 'requests_per_second': -1.0, 'version_conflicts': 0, 'total': 0}, 'description': 'UNIT TEST', 'running_time_in_nanos': 1637039537721, 'cancellable': True, 'action': 'indices:data/write/reindex', 'type': 'transport', 'id': 54510686, 'start_time_in_millis': 1489695981997}, 'response': {'retries': {'bulk': 0, 'search': 0}, 'updated': 0, 'batches': 0, 'throttled_until_millis': 0, 'throttled_millis': 0, 'noops': 0, 'created': 0, 'deleted': 0, 'took': 1636917, 'requests_per_second': -1.0, 'timed_out': False, 'failures': [], 'version_conflicts': 0, 'total': 0}}
recovery_output = {'index-2015.01.01': {'shards' : [{'stage':'DONE'}]}, 'index-2015.02.01': {'shards' : [{'stage':'DONE'}]}}
unrecovered_output = {'index-2015.01.01': {'shards' : [{'stage':'INDEX'}]}, 'index-2015.02.01': {'shards' : [{'stage':'INDEX'}]}}
cluster_health = { "cluster_name": "unit_test", "status": "green", "timed_out": False, "number_of_nodes": 7, "number_of_data_nodes": 3, "active_primary_shards": 235, "active_shards": 471, "relocating_shards": 0, "initializing_shards": 0, "unassigned_shards": 0, "delayed_unassigned_shards": 0, "number_of_pending_tasks": 0,  "task_max_waiting_in_queue_millis": 0, "active_shards_percent_as_number": 100}
reindex_basic = { 'source': { 'index': named_index }, 'dest': { 'index': 'other_index' } }
reindex_replace = { 'source': { 'index': 'REINDEX_SELECTION' }, 'dest': { 'index': 'other_index' } }
reindex_migration = { 'source': { 'index': named_index }, 'dest': { 'index': 'MIGRATION' } }
index_list_966 = ['indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d']
recovery_966 = {'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d': {'shards': [{'total_time': '10.1m', 'index': {'files': {'reused': 0, 'total': 15, 'percent': '100.0%', 'recovered': 15}, 'total_time': '10.1m', 'target_throttle_time': '-1', 'total_time_in_millis': 606577, 'source_throttle_time_in_millis': 0, 'source_throttle_time': '-1', 'target_throttle_time_in_millis': 0, 'size': {'recovered_in_bytes': 3171596177, 'reused': '0b', 'total_in_bytes': 3171596177, 'percent': '100.0%', 'reused_in_bytes': 0, 'total': '2.9gb', 'recovered': '2.9gb'}}, 'verify_index': {'total_time': '0s', 'total_time_in_millis': 0, 'check_index_time_in_millis': 0, 'check_index_time': '0s'}, 'target': {'ip': 'x.x.x.7', 'host': 'x.x.x.7', 'transport_address': 'x.x.x.7:9300', 'id': 'K4xQPaOFSWSPLwhb0P47aQ', 'name': 'staging-es5-forcem'}, 'source': {'index': 'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', 'version': '5.1.1', 'snapshot': 'force-merge', 'repository': 'force-merge'}, 'translog': {'total_time': '45ms', 'percent': '100.0%', 'total_time_in_millis': 45, 'total_on_start': 0, 'total': 0, 'recovered': 0}, 'start_time': '2017-05-16T11:54:48.183Z', 'primary': True, 'total_time_in_millis': 606631, 'stop_time_in_millis': 1494936294815, 'stop_time': '2017-05-16T12:04:54.815Z', 'stage': 'DONE', 'type': 'SNAPSHOT', 'id': 1, 'start_time_in_millis': 1494935688183}, {'total_time': '10m', 'index': {'files': {'reused': 0, 'total': 15, 'percent': '100.0%', 'recovered': 15}, 'total_time': '10m', 'target_throttle_time': '-1', 'total_time_in_millis': 602302, 'source_throttle_time_in_millis': 0, 'source_throttle_time': '-1', 'target_throttle_time_in_millis': 0, 'size': {'recovered_in_bytes': 3162299781, 'reused': '0b', 'total_in_bytes': 3162299781, 'percent': '100.0%', 'reused_in_bytes': 0, 'total': '2.9gb', 'recovered': '2.9gb'}}, 'verify_index': {'total_time': '0s', 'total_time_in_millis': 0, 'check_index_time_in_millis': 0, 'check_index_time': '0s'}, 'target': {'ip': 'x.x.x.7', 'host': 'x.x.x.7', 'transport_address': 'x.x.x.7:9300', 'id': 'K4xQPaOFSWSPLwhb0P47aQ', 'name': 'staging-es5-forcem'}, 'source': {'index': 'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', 'version': '5.1.1', 'snapshot': 'force-merge', 'repository': 'force-merge'}, 'translog': {'total_time': '389ms', 'percent': '100.0%', 'total_time_in_millis': 389, 'total_on_start': 0, 'total': 0, 'recovered': 0}, 'start_time': '2017-05-16T12:04:51.606Z', 'primary': True, 'total_time_in_millis': 602698, 'stop_time_in_millis': 1494936894305, 'stop_time': '2017-05-16T12:14:54.305Z', 'stage': 'DONE', 'type': 'SNAPSHOT', 'id': 5, 'start_time_in_millis': 1494936291606}, {'total_time': '10.1m', 'index': {'files': {'reused': 0, 'total': 15, 'percent': '100.0%', 'recovered': 15}, 'total_time': '10.1m', 'target_throttle_time': '-1', 'total_time_in_millis': 606692, 'source_throttle_time_in_millis': 0, 'source_throttle_time': '-1', 'target_throttle_time_in_millis': 0, 'size': {'recovered_in_bytes': 3156050994, 'reused': '0b', 'total_in_bytes': 3156050994, 'percent': '100.0%', 'reused_in_bytes': 0, 'total': '2.9gb', 'recovered': '2.9gb'}}, 'verify_index': {'total_time': '0s', 'total_time_in_millis': 0, 'check_index_time_in_millis': 0, 'check_index_time': '0s'}, 'target': {'ip': 'x.x.x.7', 'host': 'x.x.x.7', 'transport_address': 'x.x.x.7:9300', 'id': 'K4xQPaOFSWSPLwhb0P47aQ', 'name': 'staging-es5-forcem'}, 'source': {'index': 'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', 'version': '5.1.1', 'snapshot': 'force-merge', 'repository': 'force-merge'}, 'translog': {'total_time': '38ms', 'percent': '100.0%', 'total_time_in_millis': 38, 'total_on_start': 0, 'total': 0, 'recovered': 0}, 'start_time': '2017-05-16T11:54:48.166Z', 'primary': True, 'total_time_in_millis': 606737, 'stop_time_in_millis': 1494936294904, 'stop_time': '2017-05-16T12:04:54.904Z', 'stage': 'DONE', 'type': 'SNAPSHOT', 'id': 3, 'start_time_in_millis': 1494935688166}, {'total_time': '10m', 'index': {'files': {'reused': 0, 'total': 15, 'percent': '100.0%', 'recovered': 15}, 'total_time': '10m', 'target_throttle_time': '-1', 'total_time_in_millis': 602010, 'source_throttle_time_in_millis': 0, 'source_throttle_time': '-1', 'target_throttle_time_in_millis': 0, 'size': {'recovered_in_bytes': 3153017440, 'reused': '0b', 'total_in_bytes': 3153017440, 'percent': '100.0%', 'reused_in_bytes': 0, 'total': '2.9gb', 'recovered': '2.9gb'}}, 'verify_index': {'total_time': '0s', 'total_time_in_millis': 0, 'check_index_time_in_millis': 0, 'check_index_time': '0s'}, 'target': {'ip': 'x.x.x.7', 'host': 'x.x.x.7', 'transport_address': 'x.x.x.7:9300', 'id': 'K4xQPaOFSWSPLwhb0P47aQ', 'name': 'staging-es5-forcem'}, 'source': {'index': 'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', 'version': '5.1.1', 'snapshot': 'force-merge', 'repository': 'force-merge'}, 'translog': {'total_time': '558ms', 'percent': '100.0%', 'total_time_in_millis': 558, 'total_on_start': 0, 'total': 0, 'recovered': 0}, 'start_time': '2017-05-16T12:04:51.369Z', 'primary': True, 'total_time_in_millis': 602575, 'stop_time_in_millis': 1494936893944, 'stop_time': '2017-05-16T12:14:53.944Z', 'stage': 'DONE', 'type': 'SNAPSHOT', 'id': 4, 'start_time_in_millis': 1494936291369}, {'total_time': '10m', 'index': {'files': {'reused': 0, 'total': 15, 'percent': '100.0%', 'recovered': 15}, 'total_time': '10m', 'target_throttle_time': '-1', 'total_time_in_millis': 600492, 'source_throttle_time_in_millis': 0, 'source_throttle_time': '-1', 'target_throttle_time_in_millis': 0, 'size': {'recovered_in_bytes': 3153347402, 'reused': '0b', 'total_in_bytes': 3153347402, 'percent': '100.0%', 'reused_in_bytes': 0, 'total': '2.9gb', 'recovered': '2.9gb'}}, 'verify_index': {'total_time': '0s', 'total_time_in_millis': 0, 'check_index_time_in_millis': 0, 'check_index_time': '0s'}, 'target': {'ip': 'x.x.x.7', 'host': 'x.x.x.7', 'transport_address': 'x.x.x.7:9300', 'id': 'K4xQPaOFSWSPLwhb0P47aQ', 'name': 'staging-es5-forcem'}, 'source': {'index': 'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', 'version': '5.1.1', 'snapshot': 'force-merge', 'repository': 'force-merge'}, 'translog': {'total_time': '445ms', 'percent': '100.0%', 'total_time_in_millis': 445, 'total_on_start': 0, 'total': 0, 'recovered': 0}, 'start_time': '2017-05-16T12:04:54.817Z', 'primary': True, 'total_time_in_millis': 600946, 'stop_time_in_millis': 1494936895764, 'stop_time': '2017-05-16T12:14:55.764Z', 'stage': 'DONE', 'type': 'SNAPSHOT', 'id': 6, 'start_time_in_millis': 1494936294817}, {'total_time': '10m', 'index': {'files': {'reused': 0, 'total': 15, 'percent': '100.0%', 'recovered': 15}, 'total_time': '10m', 'target_throttle_time': '-1', 'total_time_in_millis': 603194, 'source_throttle_time_in_millis': 0, 'source_throttle_time': '-1', 'target_throttle_time_in_millis': 0, 'size': {'recovered_in_bytes': 3148003580, 'reused': '0b', 'total_in_bytes': 3148003580, 'percent': '100.0%', 'reused_in_bytes': 0, 'total': '2.9gb', 'recovered': '2.9gb'}}, 'verify_index': {'total_time': '0s', 'total_time_in_millis': 0, 'check_index_time_in_millis': 0, 'check_index_time': '0s'}, 'target': {'ip': 'x.x.x.7', 'host': 'x.x.x.7', 'transport_address': 'x.x.x.7:9300', 'id': 'K4xQPaOFSWSPLwhb0P47aQ', 'name': 'staging-es5-forcem'}, 'source': {'index': 'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', 'version': '5.1.1', 'snapshot': 'force-merge', 'repository': 'force-merge'}, 'translog': {'total_time': '225ms', 'percent': '100.0%', 'total_time_in_millis': 225, 'total_on_start': 0, 'total': 0, 'recovered': 0}, 'start_time': '2017-05-16T11:54:48.173Z', 'primary': True, 'total_time_in_millis': 603429, 'stop_time_in_millis': 1494936291602, 'stop_time': '2017-05-16T12:04:51.602Z', 'stage': 'DONE', 'type': 'SNAPSHOT', 'id': 2, 'start_time_in_millis': 1494935688173}, {'total_time': '10m', 'index': {'files': {'reused': 0, 'total': 15, 'percent': '100.0%', 'recovered': 15}, 'total_time': '10m', 'target_throttle_time': '-1', 'total_time_in_millis': 601453, 'source_throttle_time_in_millis': 0, 'source_throttle_time': '-1', 'target_throttle_time_in_millis': 0, 'size': {'recovered_in_bytes': 3168132171, 'reused': '0b', 'total_in_bytes': 3168132171, 'percent': '100.0%', 'reused_in_bytes': 0, 'total': '2.9gb', 'recovered': '2.9gb'}}, 'verify_index': {'total_time': '0s', 'total_time_in_millis': 0, 'check_index_time_in_millis': 0, 'check_index_time': '0s'}, 'target': {'ip': 'x.x.x.7', 'host': 'x.x.x.7', 'transport_address': 'x.x.x.7:9300', 'id': 'K4xQPaOFSWSPLwhb0P47aQ', 'name': 'staging-es5-forcem'}, 'source': {'index': 'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', 'version': '5.1.1', 'snapshot': 'force-merge', 'repository': 'force-merge'}, 'translog': {'total_time': '43ms', 'percent': '100.0%', 'total_time_in_millis': 43, 'total_on_start': 0, 'total': 0, 'recovered': 0}, 'start_time': '2017-05-16T12:04:54.905Z', 'primary': True, 'total_time_in_millis': 601503, 'stop_time_in_millis': 1494936896408, 'stop_time': '2017-05-16T12:14:56.408Z', 'stage': 'DONE', 'type': 'SNAPSHOT', 'id': 7, 'start_time_in_millis': 1494936294905}, {'total_time': '10m', 'index': {'files': {'reused': 0, 'total': 15, 'percent': '100.0%', 'recovered': 15}, 'total_time': '10m', 'target_throttle_time': '-1', 'total_time_in_millis': 602897, 'source_throttle_time_in_millis': 0, 'source_throttle_time': '-1', 'target_throttle_time_in_millis': 0, 'size': {'recovered_in_bytes': 3153750393, 'reused': '0b', 'total_in_bytes': 3153750393, 'percent': '100.0%', 'reused_in_bytes': 0, 'total': '2.9gb', 'recovered': '2.9gb'}}, 'verify_index': {'total_time': '0s', 'total_time_in_millis': 0, 'check_index_time_in_millis': 0, 'check_index_time': '0s'}, 'target': {'ip': 'x.x.x.7', 'host': 'x.x.x.7', 'transport_address': 'x.x.x.7:9300', 'id': 'K4xQPaOFSWSPLwhb0P47aQ', 'name': 'staging-es5-forcem'}, 'source': {'index': 'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', 'version': '5.1.1', 'snapshot': 'force-merge', 'repository': 'force-merge'}, 'translog': {'total_time': '271ms', 'percent': '100.0%', 'total_time_in_millis': 271, 'total_on_start': 0, 'total': 0, 'recovered': 0}, 'start_time': '2017-05-16T11:54:48.191Z', 'primary': True, 'total_time_in_millis': 603174, 'stop_time_in_millis': 1494936291366, 'stop_time': '2017-05-16T12:04:51.366Z', 'stage': 'DONE', 'type': 'SNAPSHOT', 'id': 0, 'start_time_in_millis': 1494935688191}]}}
no_snap_tasks = {'nodes': {'node1': {'tasks': {'task1': {'action': 'cluster:monitor/tasks/lists[n]'}}}}}
snap_task = {'nodes': {'node1': {'tasks': {'task1': {'action': 'cluster:admin/snapshot/delete'}}}}}
watermark_persistent = {'persistent':{'cluster':{'routing':{'allocation':{'disk':{'watermark':{'low':'11%','high':'60gb'}}}}}}}
watermark_transient = {'transient':{'cluster':{'routing':{'allocation':{'disk':{'watermark':{'low':'9%','high':'50gb'}}}}}}}
watermark_both = {
    'persistent': {'cluster':{'routing':{'allocation':{'disk':{'watermark':{'low':'11%','high':'60gb'}}}}}},
    'transient': {'cluster':{'routing':{'allocation':{'disk':{'watermark':{'low':'9%','high':'50gb'}}}}}},
}
empty_cluster_settings = {'persistent':{},'transient':{}}
data_only_node_role = ['data']
master_data_node_role = ['data','master']

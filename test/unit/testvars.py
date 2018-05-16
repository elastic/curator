import elasticsearch

fake_fail      = Exception('Simulated Failure')
four_oh_one    = elasticsearch.TransportError(401, "simulated error")
four_oh_four   = elasticsearch.TransportError(404, "simulated error")
get_alias_fail = elasticsearch.NotFoundError(404, "simulated error")
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
sync_conflict   = elasticsearch.ConflictError(409, u'{"_shards":{"total":1,"successful":0,"failed":1},"index_name":{"total":1,"successful":0,"failed":1,"failures":[{"shard":0,"reason":"pending operations","routing":{"state":"STARTED","primary":true,"node":"nodeid1","relocating_node":null,"shard":0,"index":"index_name"}}]}})', synced_fail)
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
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'random_uuid_string_here',
                u'number_of_shards': u'2', u'creation_date': u'1456963200172',
                u'routing': {u'allocation': {u'include': {u'tag': u'foo'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    }
}

settings_1_get_aliases = { named_index: { "aliases" : { 'my_alias' : { } } } }

settings_two  = {
    u'index-2016.03.03': {
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'random_uuid_string_here',
                u'number_of_shards': u'5', u'creation_date': u'1456963200172',
                u'routing': {u'allocation': {u'include': {u'tag': u'foo'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    },
    u'index-2016.03.04': {
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'another_random_uuid_string',
                u'number_of_shards': u'5', u'creation_date': u'1457049600812',
                u'routing': {u'allocation': {u'include': {u'tag': u'bar'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    }
}

settings_2_get_aliases = {
    "index-2016.03.03": { "aliases" : { 'my_alias' : { } } },
    "index-2016.03.04": { "aliases" : { 'my_alias' : { } } },
}

settings_2_closed = {
    u'index-2016.03.03': {
        u'state': u'close',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'random_uuid_string_here',
                u'number_of_shards': u'5', u'creation_date': u'1456963200172',
                u'routing': {u'allocation': {u'include': {u'tag': u'foo'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    },
    u'index-2016.03.04': {
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'another_random_uuid_string',
                u'number_of_shards': u'5', u'creation_date': u'1457049600812',
                u'routing': {u'allocation': {u'include': {u'tag': u'bar'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    }
}

settings_two_no_cd  = {
    u'index-2016.03.03': {
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'random_uuid_string_here',
                u'number_of_shards': u'5', u'creation_date': u'1456963200172',
                u'routing': {u'allocation': {u'include': {u'tag': u'foo'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    },
    u'index-2016.03.04': {
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'another_random_uuid_string',
                u'number_of_shards': u'5',
                u'routing': {u'allocation': {u'include': {u'tag': u'bar'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    }
}

settings_four  = {
    u'a-2016.03.03': {
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'random_uuid_string_here',
                u'number_of_shards': u'5', u'creation_date': u'1456963200172',
                u'routing': {u'allocation': {u'include': {u'tag': u'foo'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    },
    u'b-2016.03.04': {
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'another_random_uuid_string',
                u'number_of_shards': u'5', u'creation_date': u'1457049600812',
                u'routing': {u'allocation': {u'include': {u'tag': u'bar'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    },
    u'c-2016.03.05': {
        u'state': u'close',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'random_uuid_string_here',
                u'number_of_shards': u'5', u'creation_date': u'1457136000933',
                u'routing': {u'allocation': {u'include': {u'tag': u'foo'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    },
    u'd-2016.03.06': {
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'another_random_uuid_string',
                u'number_of_shards': u'5', u'creation_date': u'1457222400527',
                u'routing': {u'allocation': {u'include': {u'tag': u'bar'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    }
}

settings_named = {
    u'index-2015.01.01': {
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'random_uuid_string_here',
                u'number_of_shards': u'5', u'creation_date': u'1456963200172',
                u'routing': {u'allocation': {u'include': {u'tag': u'foo'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    },
    u'index-2015.02.01': {
        u'state': u'open',
        u'aliases': [u'my_alias'],
        u'mappings': {},
        u'settings': {
            u'index': {
                u'number_of_replicas': u'1', u'uuid': u'another_random_uuid_string',
                u'number_of_shards': u'5', u'creation_date': u'1457049600812',
                u'routing': {u'allocation': {u'include': {u'tag': u'bar'}}},
                u'version': {u'created': u'2020099'}, u'refresh_interval': u'5s'
            }
        }
    }
}

clu_state_one  = {
    u'metadata': {
        u'indices': settings_one
    }
}
clu_state_two  = {
    u'metadata': {
        u'indices': settings_two
    }
}
cs_two_closed  = {
    u'metadata': {
        u'indices': settings_2_closed
    }
}
clu_state_two_no_cd  = {
    u'metadata': {
        u'indices': settings_two_no_cd
    }
}
clu_state_four = {
    u'metadata': {
        u'indices': settings_four
    }
}

stats_one      = {
    u'indices': {
        named_index : {
            u'total': {
                u'docs': {u'count': 6374962, u'deleted': 0},
                u'store': {u'size_in_bytes': 1115219663, u'throttle_time_in_millis': 0}
            },
            u'primaries': {
                u'docs': {u'count': 3187481, u'deleted': 0},
                u'store': {u'size_in_bytes': 557951789, u'throttle_time_in_millis': 0}
            }
        }
    }
}

stats_two      = {
    u'indices': {
        u'index-2016.03.03': {
            u'total': {
                u'docs': {u'count': 6374962, u'deleted': 0},
                u'store': {u'size_in_bytes': 1115219663, u'throttle_time_in_millis': 0}
            },
            u'primaries': {
                u'docs': {u'count': 3187481, u'deleted': 0},
                u'store': {u'size_in_bytes': 557951789, u'throttle_time_in_millis': 0}
            }
        },
        u'index-2016.03.04': {
            u'total': {
                u'docs': {u'count': 6377544, u'deleted': 0},
                u'store': {u'size_in_bytes': 1120891046, u'throttle_time_in_millis': 0}
            },
            u'primaries': {
                u'docs': {u'count': 3188772, u'deleted': 0},
                u'store': {u'size_in_bytes': 560677114, u'throttle_time_in_millis': 0}
            }
        }
    }
}

stats_four      = {
    u'indices': {
        u'a-2016.03.03': {
            u'total': {
                u'docs': {u'count': 6374962, u'deleted': 0},
                u'store': {u'size_in_bytes': 1115219663, u'throttle_time_in_millis': 0}
            },
            u'primaries': {
                u'docs': {u'count': 3187481, u'deleted': 0},
                u'store': {u'size_in_bytes': 557951789, u'throttle_time_in_millis': 0}
            }
        },
        u'b-2016.03.04': {
            u'total': {
                u'docs': {u'count': 6377544, u'deleted': 0},
                u'store': {u'size_in_bytes': 1120891046, u'throttle_time_in_millis': 0}
            },
            u'primaries': {
                u'docs': {u'count': 3188772, u'deleted': 0},
                u'store': {u'size_in_bytes': 560677114, u'throttle_time_in_millis': 0}
            }
        },
        # CLOSED, ergo, not present
        # u'c-2016.03.05': {
        #     u'total': {
        #         u'docs': {u'count': 6266434, u'deleted': 0},
        #         u'store': {u'size_in_bytes': 1120882166, u'throttle_time_in_millis': 0}
        #     },
        #     u'primaries': {
        #         u'docs': {u'count': 3133217, u'deleted': 0},
        #         u'store': {u'size_in_bytes': 560441083, u'throttle_time_in_millis': 0}
        #     }
        # },
        u'd-2016.03.06': {
            u'total': {
                u'docs': {u'count': 6266436, u'deleted': 0},
                u'store': {u'size_in_bytes': 1120882168, u'throttle_time_in_millis': 0}
            },
            u'primaries': {
                u'docs': {u'count': 3133218, u'deleted': 0},
                u'store': {u'size_in_bytes': 560441084, u'throttle_time_in_millis': 0}
            }
        }

    }
}

fieldstats_one = {
    u'indices': {
        named_index : {
            u'fields': {
                u'timestamp': {
                    u'density': 100,
                    u'min_value_as_string': u'2016-03-03T00:00:06.189Z',
                    u'max_value': 1457049599152, u'max_doc': 415651,
                    u'min_value': 1456963206189, u'doc_count': 415651,
                    u'max_value_as_string': u'2016-03-03T23:59:59.152Z',
                    u'sum_total_term_freq': -1, u'sum_doc_freq': 1662604}}}}
    }

fieldstats_two = {
    u'indices': {
        u'index-2016.03.03': {
            u'fields': {
                u'timestamp': {
                    u'density': 100,
                    u'min_value_as_string': u'2016-03-03T00:00:06.189Z',
                    u'max_value': 1457049599152, u'max_doc': 415651,
                    u'min_value': 1456963206189, u'doc_count': 415651,
                    u'max_value_as_string': u'2016-03-03T23:59:59.152Z',
                    u'sum_total_term_freq': -1, u'sum_doc_freq': 1662604}}},
        u'index-2016.03.04': {
            u'fields': {
                u'timestamp': {
                    u'density': 100,
                    u'min_value_as_string': u'2016-03-04T00:00:00.812Z',
                    u'max_value': 1457135999223, u'max_doc': 426762,
                    u'min_value': 1457049600812, u'doc_count': 426762,
                    u'max_value_as_string': u'2016-03-04T23:59:59.223Z',
                    u'sum_total_term_freq': -1, u'sum_doc_freq': 1673715}}},
    }
}

fieldstats_four = {
    u'indices': {
        u'a-2016.03.03': {
            u'fields': {
                u'timestamp': {
                    u'density': 100,
                    u'min_value_as_string': u'2016-03-03T00:00:06.189Z',
                    u'max_value': 1457049599152, u'max_doc': 415651,
                    u'min_value': 1456963206189, u'doc_count': 415651,
                    u'max_value_as_string': u'2016-03-03T23:59:59.152Z',
                    u'sum_total_term_freq': -1, u'sum_doc_freq': 1662604}}},
        u'b-2016.03.04': {
            u'fields': {
                u'timestamp': {
                    u'density': 100,
                    u'min_value_as_string': u'2016-03-04T00:00:00.812Z',
                    u'max_value': 1457135999223, u'max_doc': 426762,
                    u'min_value': 1457049600812, u'doc_count': 426762,
                    u'max_value_as_string': u'2016-03-04T23:59:59.223Z',
                    u'sum_total_term_freq': -1, u'sum_doc_freq': 1673715}}},
        u'd-2016.03.06': {
            u'fields': {
                u'timestamp': {
                    u'density': 100,
                    u'min_value_as_string': u'2016-03-04T00:00:00.812Z',
                    u'max_value': 1457308799223, u'max_doc': 426762,
                    u'min_value': 1457222400567, u'doc_count': 426762,
                    u'max_value_as_string': u'2016-03-04T23:59:59.223Z',
                    u'sum_total_term_freq': -1, u'sum_doc_freq': 1673715}}},
    }
}

fieldstats_query = {
    u'aggregations': {
        u'min' : {
            u'value_as_string': u'2016-03-03T00:00:06.189Z',
            u'value': 1456963206189,
        },
        u'max' : {
            u'value': 1457049599152,
            u'value_as_string': u'2016-03-03T23:59:59.152Z',
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
  hosts: localhost
  port: 9200
  url_prefix:
  use_ssl: False
  certificate:
  client_cert:
  client_key:
  ssl_no_validate: False
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

not_rollable_name = {'index': {u'aliases': {'foo': {}}}}
not_rollable_multiple = {u'index-a': {u'aliases': {u'foo': {}}}, u'index-b': {u'aliases': {u'foo': {}}}}
not_rollable_non_numeric = {u'index-a': {u'aliases': {u'foo': {}}}}
is_rollable_2digits = {u'index-00001': {u'aliases': {u'foo': {}}}}
is_rollable_hypenated = {u'index-2017.03.07-1': {u'aliases': {u'foo': {}}}}
generic_task = {u'task': u'I0ekFjMhSPCQz7FUs1zJOg:54510686'}
incomplete_task = {u'completed': False, u'task': {u'node': u'I0ekFjMhSPCQz7FUs1zJOg', u'status': {u'retries': {u'bulk': 0, u'search': 0}, u'updated': 0, u'batches': 3647, u'throttled_until_millis': 0, u'throttled_millis': 0, u'noops': 0, u'created': 3646581, u'deleted': 0, u'requests_per_second': -1.0, u'version_conflicts': 0, u'total': 3646581}, u'description': u'UNIT TEST', u'running_time_in_nanos': 1637039537721, u'cancellable': True, u'action': u'indices:data/write/reindex', u'type': u'transport', u'id': 54510686, u'start_time_in_millis': 1489695981997}, u'response': {u'retries': {u'bulk': 0, u'search': 0}, u'updated': 0, u'batches': 3647, u'throttled_until_millis': 0, u'throttled_millis': 0, u'noops': 0, u'created': 3646581, u'deleted': 0, u'took': 1636917, u'requests_per_second': -1.0, u'timed_out': False, u'failures': [], u'version_conflicts': 0, u'total': 3646581}}
completed_task = {u'completed': True, u'task': {u'node': u'I0ekFjMhSPCQz7FUs1zJOg', u'status': {u'retries': {u'bulk': 0, u'search': 0}, u'updated': 0, u'batches': 3647, u'throttled_until_millis': 0, u'throttled_millis': 0, u'noops': 0, u'created': 3646581, u'deleted': 0, u'requests_per_second': -1.0, u'version_conflicts': 0, u'total': 3646581}, u'description': u'UNIT TEST', u'running_time_in_nanos': 1637039537721, u'cancellable': True, u'action': u'indices:data/write/reindex', u'type': u'transport', u'id': 54510686, u'start_time_in_millis': 1489695981997}, u'response': {u'retries': {u'bulk': 0, u'search': 0}, u'updated': 0, u'batches': 3647, u'throttled_until_millis': 0, u'throttled_millis': 0, u'noops': 0, u'created': 3646581, u'deleted': 0, u'took': 1636917, u'requests_per_second': -1.0, u'timed_out': False, u'failures': [], u'version_conflicts': 0, u'total': 3646581}}
recovery_output = {'index-2015.01.01': {'shards' : [{'stage':'DONE'}]}, 'index-2015.02.01': {'shards' : [{'stage':'DONE'}]}}
unrecovered_output = {'index-2015.01.01': {'shards' : [{'stage':'INDEX'}]}, 'index-2015.02.01': {'shards' : [{'stage':'INDEX'}]}}
cluster_health = { "cluster_name": "unit_test", "status": "green", "timed_out": False, "number_of_nodes": 7, "number_of_data_nodes": 3, "active_primary_shards": 235, "active_shards": 471, "relocating_shards": 0, "initializing_shards": 0, "unassigned_shards": 0, "delayed_unassigned_shards": 0, "number_of_pending_tasks": 0,  "task_max_waiting_in_queue_millis": 0, "active_shards_percent_as_number": 100}
reindex_basic = { 'source': { 'index': named_index }, 'dest': { 'index': 'other_index' } }
reindex_replace = { 'source': { 'index': 'REINDEX_SELECTION' }, 'dest': { 'index': 'other_index' } }
reindex_migration = { 'source': { 'index': named_index }, 'dest': { 'index': 'MIGRATION' } }
index_list_966 = ['indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d']
recovery_966 = {u'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d': {u'shards': [{u'total_time': u'10.1m', u'index': {u'files': {u'reused': 0, u'total': 15, u'percent': u'100.0%', u'recovered': 15}, u'total_time': u'10.1m', u'target_throttle_time': u'-1', u'total_time_in_millis': 606577, u'source_throttle_time_in_millis': 0, u'source_throttle_time': u'-1', u'target_throttle_time_in_millis': 0, u'size': {u'recovered_in_bytes': 3171596177, u'reused': u'0b', u'total_in_bytes': 3171596177, u'percent': u'100.0%', u'reused_in_bytes': 0, u'total': u'2.9gb', u'recovered': u'2.9gb'}}, u'verify_index': {u'total_time': u'0s', u'total_time_in_millis': 0, u'check_index_time_in_millis': 0, u'check_index_time': u'0s'}, u'target': {u'ip': u'x.x.x.7', u'host': u'x.x.x.7', u'transport_address': u'x.x.x.7:9300', u'id': u'K4xQPaOFSWSPLwhb0P47aQ', u'name': u'staging-es5-forcem'}, u'source': {u'index': u'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', u'version': u'5.1.1', u'snapshot': u'force-merge', u'repository': u'force-merge'}, u'translog': {u'total_time': u'45ms', u'percent': u'100.0%', u'total_time_in_millis': 45, u'total_on_start': 0, u'total': 0, u'recovered': 0}, u'start_time': u'2017-05-16T11:54:48.183Z', u'primary': True, u'total_time_in_millis': 606631, u'stop_time_in_millis': 1494936294815, u'stop_time': u'2017-05-16T12:04:54.815Z', u'stage': u'DONE', u'type': u'SNAPSHOT', u'id': 1, u'start_time_in_millis': 1494935688183}, {u'total_time': u'10m', u'index': {u'files': {u'reused': 0, u'total': 15, u'percent': u'100.0%', u'recovered': 15}, u'total_time': u'10m', u'target_throttle_time': u'-1', u'total_time_in_millis': 602302, u'source_throttle_time_in_millis': 0, u'source_throttle_time': u'-1', u'target_throttle_time_in_millis': 0, u'size': {u'recovered_in_bytes': 3162299781, u'reused': u'0b', u'total_in_bytes': 3162299781, u'percent': u'100.0%', u'reused_in_bytes': 0, u'total': u'2.9gb', u'recovered': u'2.9gb'}}, u'verify_index': {u'total_time': u'0s', u'total_time_in_millis': 0, u'check_index_time_in_millis': 0, u'check_index_time': u'0s'}, u'target': {u'ip': u'x.x.x.7', u'host': u'x.x.x.7', u'transport_address': u'x.x.x.7:9300', u'id': u'K4xQPaOFSWSPLwhb0P47aQ', u'name': u'staging-es5-forcem'}, u'source': {u'index': u'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', u'version': u'5.1.1', u'snapshot': u'force-merge', u'repository': u'force-merge'}, u'translog': {u'total_time': u'389ms', u'percent': u'100.0%', u'total_time_in_millis': 389, u'total_on_start': 0, u'total': 0, u'recovered': 0}, u'start_time': u'2017-05-16T12:04:51.606Z', u'primary': True, u'total_time_in_millis': 602698, u'stop_time_in_millis': 1494936894305, u'stop_time': u'2017-05-16T12:14:54.305Z', u'stage': u'DONE', u'type': u'SNAPSHOT', u'id': 5, u'start_time_in_millis': 1494936291606}, {u'total_time': u'10.1m', u'index': {u'files': {u'reused': 0, u'total': 15, u'percent': u'100.0%', u'recovered': 15}, u'total_time': u'10.1m', u'target_throttle_time': u'-1', u'total_time_in_millis': 606692, u'source_throttle_time_in_millis': 0, u'source_throttle_time': u'-1', u'target_throttle_time_in_millis': 0, u'size': {u'recovered_in_bytes': 3156050994, u'reused': u'0b', u'total_in_bytes': 3156050994, u'percent': u'100.0%', u'reused_in_bytes': 0, u'total': u'2.9gb', u'recovered': u'2.9gb'}}, u'verify_index': {u'total_time': u'0s', u'total_time_in_millis': 0, u'check_index_time_in_millis': 0, u'check_index_time': u'0s'}, u'target': {u'ip': u'x.x.x.7', u'host': u'x.x.x.7', u'transport_address': u'x.x.x.7:9300', u'id': u'K4xQPaOFSWSPLwhb0P47aQ', u'name': u'staging-es5-forcem'}, u'source': {u'index': u'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', u'version': u'5.1.1', u'snapshot': u'force-merge', u'repository': u'force-merge'}, u'translog': {u'total_time': u'38ms', u'percent': u'100.0%', u'total_time_in_millis': 38, u'total_on_start': 0, u'total': 0, u'recovered': 0}, u'start_time': u'2017-05-16T11:54:48.166Z', u'primary': True, u'total_time_in_millis': 606737, u'stop_time_in_millis': 1494936294904, u'stop_time': u'2017-05-16T12:04:54.904Z', u'stage': u'DONE', u'type': u'SNAPSHOT', u'id': 3, u'start_time_in_millis': 1494935688166}, {u'total_time': u'10m', u'index': {u'files': {u'reused': 0, u'total': 15, u'percent': u'100.0%', u'recovered': 15}, u'total_time': u'10m', u'target_throttle_time': u'-1', u'total_time_in_millis': 602010, u'source_throttle_time_in_millis': 0, u'source_throttle_time': u'-1', u'target_throttle_time_in_millis': 0, u'size': {u'recovered_in_bytes': 3153017440, u'reused': u'0b', u'total_in_bytes': 3153017440, u'percent': u'100.0%', u'reused_in_bytes': 0, u'total': u'2.9gb', u'recovered': u'2.9gb'}}, u'verify_index': {u'total_time': u'0s', u'total_time_in_millis': 0, u'check_index_time_in_millis': 0, u'check_index_time': u'0s'}, u'target': {u'ip': u'x.x.x.7', u'host': u'x.x.x.7', u'transport_address': u'x.x.x.7:9300', u'id': u'K4xQPaOFSWSPLwhb0P47aQ', u'name': u'staging-es5-forcem'}, u'source': {u'index': u'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', u'version': u'5.1.1', u'snapshot': u'force-merge', u'repository': u'force-merge'}, u'translog': {u'total_time': u'558ms', u'percent': u'100.0%', u'total_time_in_millis': 558, u'total_on_start': 0, u'total': 0, u'recovered': 0}, u'start_time': u'2017-05-16T12:04:51.369Z', u'primary': True, u'total_time_in_millis': 602575, u'stop_time_in_millis': 1494936893944, u'stop_time': u'2017-05-16T12:14:53.944Z', u'stage': u'DONE', u'type': u'SNAPSHOT', u'id': 4, u'start_time_in_millis': 1494936291369}, {u'total_time': u'10m', u'index': {u'files': {u'reused': 0, u'total': 15, u'percent': u'100.0%', u'recovered': 15}, u'total_time': u'10m', u'target_throttle_time': u'-1', u'total_time_in_millis': 600492, u'source_throttle_time_in_millis': 0, u'source_throttle_time': u'-1', u'target_throttle_time_in_millis': 0, u'size': {u'recovered_in_bytes': 3153347402, u'reused': u'0b', u'total_in_bytes': 3153347402, u'percent': u'100.0%', u'reused_in_bytes': 0, u'total': u'2.9gb', u'recovered': u'2.9gb'}}, u'verify_index': {u'total_time': u'0s', u'total_time_in_millis': 0, u'check_index_time_in_millis': 0, u'check_index_time': u'0s'}, u'target': {u'ip': u'x.x.x.7', u'host': u'x.x.x.7', u'transport_address': u'x.x.x.7:9300', u'id': u'K4xQPaOFSWSPLwhb0P47aQ', u'name': u'staging-es5-forcem'}, u'source': {u'index': u'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', u'version': u'5.1.1', u'snapshot': u'force-merge', u'repository': u'force-merge'}, u'translog': {u'total_time': u'445ms', u'percent': u'100.0%', u'total_time_in_millis': 445, u'total_on_start': 0, u'total': 0, u'recovered': 0}, u'start_time': u'2017-05-16T12:04:54.817Z', u'primary': True, u'total_time_in_millis': 600946, u'stop_time_in_millis': 1494936895764, u'stop_time': u'2017-05-16T12:14:55.764Z', u'stage': u'DONE', u'type': u'SNAPSHOT', u'id': 6, u'start_time_in_millis': 1494936294817}, {u'total_time': u'10m', u'index': {u'files': {u'reused': 0, u'total': 15, u'percent': u'100.0%', u'recovered': 15}, u'total_time': u'10m', u'target_throttle_time': u'-1', u'total_time_in_millis': 603194, u'source_throttle_time_in_millis': 0, u'source_throttle_time': u'-1', u'target_throttle_time_in_millis': 0, u'size': {u'recovered_in_bytes': 3148003580, u'reused': u'0b', u'total_in_bytes': 3148003580, u'percent': u'100.0%', u'reused_in_bytes': 0, u'total': u'2.9gb', u'recovered': u'2.9gb'}}, u'verify_index': {u'total_time': u'0s', u'total_time_in_millis': 0, u'check_index_time_in_millis': 0, u'check_index_time': u'0s'}, u'target': {u'ip': u'x.x.x.7', u'host': u'x.x.x.7', u'transport_address': u'x.x.x.7:9300', u'id': u'K4xQPaOFSWSPLwhb0P47aQ', u'name': u'staging-es5-forcem'}, u'source': {u'index': u'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', u'version': u'5.1.1', u'snapshot': u'force-merge', u'repository': u'force-merge'}, u'translog': {u'total_time': u'225ms', u'percent': u'100.0%', u'total_time_in_millis': 225, u'total_on_start': 0, u'total': 0, u'recovered': 0}, u'start_time': u'2017-05-16T11:54:48.173Z', u'primary': True, u'total_time_in_millis': 603429, u'stop_time_in_millis': 1494936291602, u'stop_time': u'2017-05-16T12:04:51.602Z', u'stage': u'DONE', u'type': u'SNAPSHOT', u'id': 2, u'start_time_in_millis': 1494935688173}, {u'total_time': u'10m', u'index': {u'files': {u'reused': 0, u'total': 15, u'percent': u'100.0%', u'recovered': 15}, u'total_time': u'10m', u'target_throttle_time': u'-1', u'total_time_in_millis': 601453, u'source_throttle_time_in_millis': 0, u'source_throttle_time': u'-1', u'target_throttle_time_in_millis': 0, u'size': {u'recovered_in_bytes': 3168132171, u'reused': u'0b', u'total_in_bytes': 3168132171, u'percent': u'100.0%', u'reused_in_bytes': 0, u'total': u'2.9gb', u'recovered': u'2.9gb'}}, u'verify_index': {u'total_time': u'0s', u'total_time_in_millis': 0, u'check_index_time_in_millis': 0, u'check_index_time': u'0s'}, u'target': {u'ip': u'x.x.x.7', u'host': u'x.x.x.7', u'transport_address': u'x.x.x.7:9300', u'id': u'K4xQPaOFSWSPLwhb0P47aQ', u'name': u'staging-es5-forcem'}, u'source': {u'index': u'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', u'version': u'5.1.1', u'snapshot': u'force-merge', u'repository': u'force-merge'}, u'translog': {u'total_time': u'43ms', u'percent': u'100.0%', u'total_time_in_millis': 43, u'total_on_start': 0, u'total': 0, u'recovered': 0}, u'start_time': u'2017-05-16T12:04:54.905Z', u'primary': True, u'total_time_in_millis': 601503, u'stop_time_in_millis': 1494936896408, u'stop_time': u'2017-05-16T12:14:56.408Z', u'stage': u'DONE', u'type': u'SNAPSHOT', u'id': 7, u'start_time_in_millis': 1494936294905}, {u'total_time': u'10m', u'index': {u'files': {u'reused': 0, u'total': 15, u'percent': u'100.0%', u'recovered': 15}, u'total_time': u'10m', u'target_throttle_time': u'-1', u'total_time_in_millis': 602897, u'source_throttle_time_in_millis': 0, u'source_throttle_time': u'-1', u'target_throttle_time_in_millis': 0, u'size': {u'recovered_in_bytes': 3153750393, u'reused': u'0b', u'total_in_bytes': 3153750393, u'percent': u'100.0%', u'reused_in_bytes': 0, u'total': u'2.9gb', u'recovered': u'2.9gb'}}, u'verify_index': {u'total_time': u'0s', u'total_time_in_millis': 0, u'check_index_time_in_millis': 0, u'check_index_time': u'0s'}, u'target': {u'ip': u'x.x.x.7', u'host': u'x.x.x.7', u'transport_address': u'x.x.x.7:9300', u'id': u'K4xQPaOFSWSPLwhb0P47aQ', u'name': u'staging-es5-forcem'}, u'source': {u'index': u'indexv0.2_2017-02-12_536a9247f9fa4fc7a7942ad46ea14e0d', u'version': u'5.1.1', u'snapshot': u'force-merge', u'repository': u'force-merge'}, u'translog': {u'total_time': u'271ms', u'percent': u'100.0%', u'total_time_in_millis': 271, u'total_on_start': 0, u'total': 0, u'recovered': 0}, u'start_time': u'2017-05-16T11:54:48.191Z', u'primary': True, u'total_time_in_millis': 603174, u'stop_time_in_millis': 1494936291366, u'stop_time': u'2017-05-16T12:04:51.366Z', u'stage': u'DONE', u'type': u'SNAPSHOT', u'id': 0, u'start_time_in_millis': 1494935688191}]}}
no_snap_tasks = {u'nodes': {u'node1': {u'tasks': {u'task1': {u'action': u'cluster:monitor/tasks/lists[n]'}}}}}
snap_task = {u'nodes': {u'node1': {u'tasks': {u'task1': {u'action': u'cluster:admin/snapshot/delete'}}}}}
watermark_persistent = {u'persistent':{u'cluster':{u'routing':{u'allocation':{u'disk':{u'watermark':{u'low':u'11%',u'high':u'60gb'}}}}}}}
watermark_transient = {u'transient':{u'cluster':{u'routing':{u'allocation':{u'disk':{u'watermark':{u'low':u'9%',u'high':u'50gb'}}}}}}}
watermark_both = {
    u'persistent': {u'cluster':{u'routing':{u'allocation':{u'disk':{u'watermark':{u'low':u'11%',u'high':u'60gb'}}}}}},
    u'transient': {u'cluster':{u'routing':{u'allocation':{u'disk':{u'watermark':{u'low':u'9%',u'high':u'50gb'}}}}}},
}
empty_cluster_settings = {u'persistent':{},u'transient':{}}
data_only_node_role = ['data']
master_data_node_role = ['data','master']

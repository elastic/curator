import logging
from curator.defaults import filter_elements, settings

logger = logging.getLogger(__name__)

## Helpers ##

def _age_elements(action, config):
    retval = []
    is_req = True
    if config['filtertype'] in ['count', 'space']:
        is_req = True if 'use_age' in config and config['use_age'] else False
    retval.append(filter_elements.source(action=action, required=is_req))
    if action in settings.index_actions():
        retval.append(filter_elements.stats_result())
    # This is a silly thing here, because the absence of 'source' will
    # show up in the actual schema check, but it keeps code from breaking here
    ts_req = False
    if 'source' in config:
        if config['source'] == 'name':
            ts_req = True
        elif action in settings.index_actions():
            # field_stats must _only_ exist for Index actions (not Snapshot)
            if config['source'] == 'field_stats':
                retval.append(filter_elements.field(required=True))
            else:
                retval.append(filter_elements.field(required=False))
        retval.append(filter_elements.timestring(required=ts_req))
    else:
        # If source isn't in the config, then the other elements are not
        # required, but should be Optional to prevent false positives
        retval.append(filter_elements.field(required=False))
        retval.append(filter_elements.timestring(required=ts_req))
    return retval

### Schema information ###

def alias(action, config):
    return [
        filter_elements.aliases(),
        filter_elements.exclude(),
    ]

def age(action, config):
    # Required & Optional
    retval = [
        filter_elements.direction(),
        filter_elements.unit(),
        filter_elements.unit_count(),
        filter_elements.unit_count_pattern(),
        filter_elements.epoch(),
        filter_elements.exclude(),
    ]
    retval += _age_elements(action, config)
    logger.debug('AGE FILTER = {0}'.format(retval))
    return retval

def allocated(action, config):
    return [
        filter_elements.key(),
        filter_elements.value(),
        filter_elements.allocation_type(),
        filter_elements.exclude(exclude=True),
    ]

def closed(action, config):
    return [ filter_elements.exclude(exclude=True) ]

def count(action, config):
    retval = [
        filter_elements.count(),
        filter_elements.use_age(),
        filter_elements.pattern(),
        filter_elements.reverse(),
        filter_elements.exclude(exclude=True),
    ]
    retval += _age_elements(action, config)
    return retval

def forcemerged(action, config):
    return [
        filter_elements.max_num_segments(),
        filter_elements.exclude(exclude=True),
    ]

def ilm(action, config):
    return [ filter_elements.exclude(exclude=True) ]

def kibana(action, config):
    return [ filter_elements.exclude(exclude=True) ]

def none(action, config):
    return [ ]

def opened(action, config):
    return [ filter_elements.exclude(exclude=True) ]

def pattern(action, config):
    return [
        filter_elements.kind(),
        filter_elements.value(),
        filter_elements.exclude(),
    ]

def period(action, config):
    retval = [
        filter_elements.unit(period=True),
        filter_elements.range_from(),
        filter_elements.range_to(),
        filter_elements.week_starts_on(),
        filter_elements.epoch(),
        filter_elements.exclude(),
        filter_elements.period_type(),
        filter_elements.date_from(),
        filter_elements.date_from_format(),
        filter_elements.date_to(),
        filter_elements.date_to_format(),
    ]
    # Only add intersect() to index actions.
    if action in settings.index_actions():
        retval.append(filter_elements.intersect())
    retval += _age_elements(action, config)
    return retval

def space(action, config):
    retval = [
        filter_elements.disk_space(),
        filter_elements.reverse(),
        filter_elements.use_age(),
        filter_elements.exclude(),
        filter_elements.threshold_behavior(),
    ]
    retval += _age_elements(action, config)
    return retval

def state(action, config):
    return [
        filter_elements.state(),
        filter_elements.exclude(),
    ]

def shards(action, config):
    return [
        filter_elements.number_of_shards(),
        filter_elements.shard_filter_behavior(),
        filter_elements.exclude(),
    ]

def empty(action, config):
    return [
        filter_elements.exclude(),
    ]

from tap_pipedrive.stream import PipedriveStream


class ActivityTypesStream(PipedriveStream):
    endpoint = 'activityTypes'
    metadata_endpoint = 'activityFields'
    schema = 'activity_types'
    key_properties = ['id']
    state_field = 'update_time'
    replication_method = 'INCREMENTAL'

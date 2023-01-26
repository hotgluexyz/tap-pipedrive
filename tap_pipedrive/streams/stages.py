from tap_pipedrive.stream import PipedriveStream


class StagesStream(PipedriveStream):
    endpoint = 'stages'
    schema = 'stages'
    key_properties = ['id', ]
    replication_method = 'INCREMENTAL'
    state_field = 'add_time'

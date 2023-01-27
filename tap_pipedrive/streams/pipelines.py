from tap_pipedrive.stream import PipedriveStream


class PipelinesStream(PipedriveStream):
    endpoint = "pipelines"
    schema = "pipelines"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    state_field = "add_time"

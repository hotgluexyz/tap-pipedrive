from tap_pipedrive.stream import PipedriveStream


class NotesStream(PipedriveStream):
    endpoint = "notes"
    metadata_endpoint = 'noteFields'
    schema = "notes"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    state_field = "update_time"

import singer
from tap_pipedrive.stream import PipedriveIterStream

class DealStageChangeStream(PipedriveIterStream):
    base_endpoint = 'deals'
    id_endpoint = 'deals/{}/flow'
    schema = 'dealflow'
    state_field = 'add_time'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'

    def get_name(self):
        return self.schema


    def update_endpoint(self, deal_id):
        self.endpoint = self.id_endpoint.format(deal_id)
import singer
from tap_pipedrive.stream import PipedriveIterStream

class DealsProductsStream(PipedriveIterStream):
    base_endpoint = 'deals'
    id_endpoint = 'deals/{}/products'
    metadata_endpoint = 'productFields'
    schema = 'deal_products'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'add_time'

    def get_name(self):
        return self.schema

    def update_endpoint(self, deal_id):
        self.endpoint = self.id_endpoint.format(deal_id)
import singer
from requests import RequestException
from tap_pipedrive.streams.recents import RecentsStream


logger = singer.get_logger()


class DynamicTypingRecentsStream(RecentsStream):
    schema_path = 'schemas/recents/dynamic_typing/{}.json'
    static_fields = []
    fields_endpoint = ''
    fields_more_items_in_collection = True
    fields_start = 0
    fields_limit = 100
    schema_mapping = {}

    def clean_string(self,string):
        return string.replace(" ", "_").replace("-", "_").replace("/", "_").replace("(", "").replace(")", "").replace(".", "").replace(",", "").replace(":", "").replace(";", "").replace("&", "and").replace("'", "").replace('"', "").lower()
    def get_fields_response(self,limit,start):
        fields_params = {"limit" : limit, "start" : start} 
        try:
            fields_response = self.tap.execute_request(endpoint=self.fields_endpoint, params=fields_params)
        except (ConnectionError, RequestException) as e:
            raise e
        return fields_response
    def get_schema_mapping(self):
        if self.schema_mapping:
            return self.schema_mapping
        else:
            self.get_schema()
            return self.schema_mapping

    def get_schema(self):
        if not self.schema_cache:
            schema = self.load_schema()

            while self.fields_more_items_in_collection:

                fields_response = self.get_fields_response(self.fields_limit,self.fields_start)
                try:
                    payload = fields_response.json() # Verifying response in execute_request

                    for property in payload['data']:
                        key = f"{property['key']}"
                        if property.get("edit_flag",False):
                            key = self.clean_string(property['name'])
                            if property.get("is_subfield"):
                                key = self.clean_string(property['name'])
                            self.schema_mapping[property['key']] = key
                        if key not in self.static_fields:
                            logger.debug(key, property['field_type'], property['mandatory_flag'])

                            if key in schema['properties']:
                                logger.warn('Dynamic property "{}" overrides with type {} existing entry in ' \
                                            'static JSON schema of {} stream.'.format(
                                                key,
                                                property['field_type'],
                                                self.schema
                                            )
                                )

                            property_content = {
                                'type': []
                            }

                            if property['field_type'] in ['int']:
                                property_content['type'].append('integer')

                            elif property['field_type'] in ['timestamp']:
                                property_content['type'].append('string')
                                property_content['format'] = 'date-time'

                            else:
                                property_content['type'].append('string')

                            # allow all dynamic properties to be null since this 
                            # happens in practice probably because a property could
                            # be marked mandatory for some amount of time and not
                            # mandatory for another amount of time
                            property_content['type'].append('null')

                            schema['properties'][key] = property_content

                    # Check for more data is available in next page
                    if 'additional_data' in payload and 'pagination' in payload['additional_data']:
                        pagination = payload['additional_data']['pagination']
                        if 'more_items_in_collection' in pagination:
                            self.fields_more_items_in_collection = pagination['more_items_in_collection']

                            if 'next_start' in pagination:
                                self.fields_start = pagination['next_start']

                    else:
                        self.fields_more_items_in_collection = False

                except Exception as e:
                    raise e

            self.schema_cache = schema
        return self.schema_cache

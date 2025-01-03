import time
import sys
import base64
from datetime import datetime
import math
import pendulum
import requests
import singer
import simplejson
import backoff
from requests.exceptions import ConnectionError, RequestException
from json import JSONDecodeError
from singer import set_currently_syncing, metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_pipedrive.exceptions import (PipedriveError, PipedriveNotFoundError, PipedriveBadRequestError, PipedriveUnauthorizedError, PipedrivePaymentRequiredError, 
                        PipedriveForbiddenError, PipedriveGoneError, PipedriveUnsupportedMediaError, PipedriveUnprocessableEntityError, PipedriveTooManyRequestsError, 
                        PipedriveTooManyRequestsInSecondError,PipedriveInternalServiceError, PipedriveNotImplementedError, PipedriveServiceUnavailableError)
from tap_pipedrive.streams import (CurrenciesStream, ActivityTypesStream, FiltersStream, StagesStream, PipelinesStream,
                      RecentNotesStream, RecentUsersStream, RecentActivitiesStream, RecentDealsStream,
                      RecentFilesStream, RecentOrganizationsStream, RecentPersonsStream, RecentProductsStream,
                      DealStageChangeStream, DealsProductsStream)
from tap_pipedrive.streams.recents.dynamic_typing import DynamicTypingRecentsStream

logger = singer.get_logger()


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": PipedriveBadRequestError,
        "message": "Request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": PipedriveUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    402: {
        "raise_exception": PipedrivePaymentRequiredError,
        "message": "Company account is not open (possible reason: trial expired, payment details not entered)."
    },
    403: {
        "raise_exception": PipedriveForbiddenError,
        "message": "Invalid authorization credentials or permissions."
    },
    404: {
        "raise_exception": PipedriveNotFoundError,
        "message": "The requested resource does not exist."
    },
    410: {
        "raise_exception": PipedriveGoneError,
        "message": "The old resource is permanently unavailable."
    },
    415: {
        "raise_exception": PipedriveUnsupportedMediaError,
        "message": "The feature is not enabled."
    },
    422: {
        "raise_exception": PipedriveUnprocessableEntityError,
        "message": "Webhook limit reached."
    },
    429: {
        "raise_exception": PipedriveTooManyRequestsError,
        "message": "Rate limit has been exceeded."
    },
    500: {
        "raise_exception": PipedriveInternalServiceError,
        "message": "Internal Service Error from PipeDrive."
    },
    501: {
        "raise_exception": PipedriveNotImplementedError,
        "message": "Functionality does not exist."
    },
    503: {
        "raise_exception": PipedriveServiceUnavailableError,
        "message": "Schedule maintenance on Pipedrive's end."
    },
}

def is_not_status_code_fn(status_code):
    def gen_fn(exc):
        if getattr(exc, 'response', None) and getattr(exc.response, 'status_code', None) and exc.response.status_code not in status_code:
            return True
        # Retry other errors up to the max
        return False
    return gen_fn

def retry_after_wait_gen():
    while True:
        # This is called in an except block so we can retrieve the exception
        # and check it.
        exc_info = sys.exc_info()
        resp = exc_info[1].response
        sleep_time_str = resp.headers.get('X-RateLimit-Reset')
        logger.info("API rate limit exceeded -- sleeping for %s seconds", sleep_time_str)
        yield math.floor(float(sleep_time_str))


class PipedriveTap(object):
    streams = [
        CurrenciesStream(),
        ActivityTypesStream(),
        StagesStream(),
        FiltersStream(),
        PipelinesStream(),
        RecentNotesStream(),
        RecentUsersStream(),
        RecentActivitiesStream(),
        RecentDealsStream(),
        RecentFilesStream(),
        RecentOrganizationsStream(),
        RecentPersonsStream(),
        RecentProductsStream(),
        DealStageChangeStream(),
        DealsProductsStream()
    ]

    def __init__(self, config, state):
        self.config = config
        self.config['start_date'] = pendulum.parse(self.config['start_date'])
        self.state = state

    def do_discover(self, return_dict=False):
        logger.info('Starting discover')

        catalog = Catalog([])
        catalog_stream_meta_dict = {}

        for stream in self.streams:
            stream.tap = self

            try:
                schema = Schema.from_dict(stream.get_schema())
            except PipedriveForbiddenError:
                logger.warning(f"Stream '{stream.get_name()}' ignored because it is not in the scopes.")
                continue
            key_properties = stream.key_properties

            meta = metadata.get_standard_metadata(
                schema=schema.to_dict(),
                key_properties=key_properties,
                valid_replication_keys=[stream.state_field] if stream.state_field else None,
                replication_method=stream.replication_method
            )

            # If the stream has a state_field, it needs to mark that property with automatic metadata
            if stream.state_field:
                meta = metadata.to_map(meta)
                if meta.get(('properties', stream.state_field)) :
                    meta[('properties', stream.state_field)]['inclusion'] = 'automatic'
                else:
                    logger.warn(f"State can't be set for {stream.schema}")
                meta = metadata.to_list(meta)

            catalog.streams.append(CatalogEntry(
                stream=stream.schema,
                tap_stream_id=stream.schema,
                key_properties=key_properties,
                schema=schema,
                metadata=meta
            ))
            catalog_stream_meta_dict[stream.schema] = meta
        if return_dict:
            cd = catalog.to_dict()
            for catalog_stream in cd.get('streams', []):
                data = []
                catalog_stream['stream_meta'] = catalog_stream_meta_dict[catalog_stream['stream']]
                try:
                    stream = next(filter(lambda stream: stream.schema == catalog_stream['stream'], self.streams))
                    if getattr(stream, 'metadata_endpoint', None):
                        response = self.execute_request(stream.metadata_endpoint)
                        res_json = response.json()
                        if 'data' in res_json:
                            data = res_json['data']
                            is_more_pages = res_json.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection', False)
                            start = 0
                            while is_more_pages:
                                start += 500
                                response = self.execute_request(stream.metadata_endpoint, {'start': start})
                                res_json = response.json()
                                data += res_json['data']
                                is_more_pages = res_json.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection', False)
                except Exception as exc:
                    logger.warning(f'Failed to find matched catalog. catalog_stream={catalog_stream} and stream={stream}. Error: {exc}')
                schema = Schema.from_dict(stream.get_schema())
                for field_key in schema.properties.keys():
                    catalog_stream['schema']['properties'][field_key]['field_meta'] = {}
                    if data:
                        try:
                            field_metadata = list(filter(lambda item: item['key'] == field_key, data))
                            if field_metadata:
                                field_metadata = field_metadata[0]
                                field_metadata['label'] = field_metadata['name']
                                catalog_stream['schema']['properties'][field_key]['field_meta'] = field_metadata
                        except Exception as exc:
                            logger.warning(f'Failed to find the field={field_key} in data. Error: {exc}')
            return cd
        return catalog

    def do_sync(self, catalog):
        logger.debug('Starting sync')

        # resuming when currently_syncing within state
        resume_from_stream = False
        if self.state and 'currently_syncing' in self.state:
            resume_from_stream = self.state['currently_syncing']

        selected_streams = self.get_selected_streams(catalog)

        if 'currently_syncing' in self.state and resume_from_stream not in selected_streams:
            resume_from_stream = False
            del self.state['currently_syncing']

        for stream in self.streams:
            if stream.schema not in selected_streams:
                continue

            stream.tap = self

            if resume_from_stream:
                if stream.schema == resume_from_stream:
                    logger.info('Resuming from {}'.format(resume_from_stream))
                    resume_from_stream = False
                else:
                    logger.info('Skipping stream {} as resuming from {}'.format(stream.schema, resume_from_stream))
                    continue

            # stream state, from state/bookmark or start_date
            stream.set_initial_state(self.state, self.config['start_date'])

            # currently syncing
            if stream.state_field:
                set_currently_syncing(self.state, stream.schema)
                self.state = singer.write_bookmark(self.state, stream.schema, stream.state_field, str(stream.initial_state))
                singer.write_state(self.state)

            # schema
            stream.write_schema()

            catalog_stream = catalog.get_stream(stream.schema)
            stream_metadata = metadata.to_map(catalog_stream.metadata)

            if stream.id_list: # see if we want to iterate over a list of deal_ids

                for deal_id in stream.get_deal_ids(self):
                    is_last_id = False

                    if deal_id == stream.these_deals[-1]: #find out if this is last deal_id in the current set
                        is_last_id = True

                    # if last page of deals, more_items in collection will be False
                    # Need to set it to True to get deal_id pagination for the first deal on the last page
                    if deal_id == stream.these_deals[0]:
                        stream.more_items_in_collection = True

                    stream.update_endpoint(deal_id)
                    stream.start = 0   # set back to zero for each new deal_id
                    self.do_paginate(stream, stream_metadata)

                    if not is_last_id:
                        stream.more_items_in_collection = True   #set back to True for pagination of next deal_id request
                    elif is_last_id and stream.more_ids_to_get:  # need to get the next batch of deal_ids
                        stream.more_items_in_collection = True
                        stream.start = stream.next_start
                    else:
                        stream.more_items_in_collection = False

                # set the attribution window so that the bookmark will reflect the new initial_state for the next sync
                stream.earliest_state = stream.stream_start.subtract(hours=3)
            else:
                # paginate
                self.do_paginate(stream, stream_metadata)

            # update state / bookmarking only when supported by stream
            if stream.state_field:
                self.state = singer.write_bookmark(self.state, stream.schema, stream.state_field,
                                                   str(stream.earliest_state))
            singer.write_state(self.state)

        # clear currently_syncing
        try:
            del self.state['currently_syncing']
        except KeyError as e:
            pass
        singer.write_state(self.state)

    def get_selected_streams(self, catalog):
        selected_streams = set()
        for stream in catalog.streams:
            mdata = metadata.to_map(stream.metadata)
            root_metadata = mdata.get(())
            if root_metadata and root_metadata.get('selected') is True:
                selected_streams.add(stream.tap_stream_id)
        return list(selected_streams)

    def do_paginate(self, stream, stream_metadata):
        while stream.has_data():

            with singer.metrics.http_request_timer(stream.schema) as timer:
                try:
                    response = self.execute_stream_request(stream)
                except (ConnectionError, RequestException) as e:
                    raise e
                timer.tags[singer.metrics.Tag.http_status_code] = response.status_code

            self.validate_response(response)
            self.rate_throttling(response)
            stream.paginate(response)

            # only dynamic type streams have get_schema_mapping()
            if isinstance(stream, DynamicTypingRecentsStream):
                schema_mapping = stream.get_schema_mapping()
            else:
                schema_mapping = stream.get_schema()

            # records with metrics
            with singer.metrics.record_counter(stream.schema) as counter:
                with singer.Transformer(singer.NO_INTEGER_DATETIME_PARSING) as optimus_prime:
                    stream_name = stream.get_name()
                    for row in self.iterate_response(response):
                        # logic to avoid duplicates HGI-6285
                        if row["id"] not in stream.ids:
                            stream.ids.append(row["id"])
                        else:
                            logger.info(f"id '{row['id']}' was previously fetched and processed for {stream_name}, skipping duplicate value...")
                            continue

                        row = stream.process_row(row)
                        if not row: # in case of a non-empty response with an empty element
                            continue
                        row_keys = list(row.keys())
                        for row_key in row_keys:
                            if row_key in schema_mapping:
                                row[schema_mapping[row_key]] = row.pop(row_key)
                        row = optimus_prime.transform(row, stream.get_schema(), stream_metadata)
                        if stream.write_record(row):
                            counter.increment()
                        stream.update_state(row)

    def iterate_response(self, response):
        payload = response.json()
        return [] if payload['data'] is None else payload['data']

    def execute_stream_request(self, stream):
        params = {
            'start': stream.start,
            'limit': stream.limit
        }
        params = stream.update_request_params(params)
        return self.execute_request(stream.endpoint, params=params)

    def get_token(self):
        url = "https://oauth.pipedrive.com/oauth/token"
        
        
        access_token = self.config.get("access_token")
        
        expires_in = self.config.get("expires_in", 0)
        
        now = round(datetime.utcnow().timestamp())

        if (not access_token) or (not expires_in) or ((expires_in - now) < 60):
            client_id = self.config.get("client_id")    
            client_secret = self.config.get("client_secret")
            refresh_token = self.config.get("refresh_token")
            secret_token = client_id + ":" + client_secret
            b64encoded = base64.b64encode(secret_token.encode()).decode()

            payload=f'grant_type=refresh_token&refresh_token={refresh_token}'
            
            headers = {
                "Authorization": f"Basic {b64encoded}",
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            response = requests.post(url, headers=headers, data=payload)
            if response.status_code > 399 and response.status_code < 500:
                raise Exception(f"Status code: {response.status_code} - {response.json()['message']}")
            
            response = response.json()
            access_token = response["access_token"]
            expires_in = now + response["expires_in"]
            refresh_token = response.get("refresh_token")

            self.config["access_token"] = access_token
            self.config["expires_in"] = expires_in

        return access_token


    @backoff.on_exception(backoff.expo, (PipedriveInternalServiceError, simplejson.scanner.JSONDecodeError, ConnectionError), max_tries = 5)
    @backoff.on_exception(retry_after_wait_gen, PipedriveTooManyRequestsInSecondError, giveup=is_not_status_code_fn([429]), jitter=None, max_tries=3)
    def execute_request(self, endpoint, params=None):
        access_token = self.get_token()
        headers = {
            # 'User-Agent': self.config['user-agent'],
            "Authorization": f"Bearer {access_token}"
        }
        _params = {
            # 'access_token': self.config['access_token'],
        }
        if params:
            _params.update(params)
        BASE_URL = f"https://{self.config['account']}.pipedrive.com/api/v1"
        url = "{}/{}".format(BASE_URL, endpoint)
        logger.debug('Firing request at {} with params: {}'.format(url, _params))
        response = requests.get(url, headers=headers, params=_params)

        if response.status_code == 200 and isinstance(response, requests.Response) :
            try:
                # Verifying json is valid or not
                response.json()
                return response
            except simplejson.scanner.JSONDecodeError as e:
                raise e
        else:
            raise_for_error(response)

    def validate_response(self, response):
        try:
            payload = response.json()
            if payload['success'] and 'data' in payload:
                return True
        except (AttributeError, simplejson.scanner.JSONDecodeError): # Verifying response in execute_request
            pass

    def rate_throttling(self, response):
        if all(x in response.headers for x in ['X-RateLimit-Remaining', 'X-RateLimit-Reset']):
            if int(response.headers['X-RateLimit-Remaining']) < 1:
                seconds_to_sleep = int(response.headers['X-RateLimit-Reset'])
                logger.debug('Hit API rate limits, no remaining requests per 10 seconds, will sleep '
                             'for {} seconds now.'.format(seconds_to_sleep))
                time.sleep(seconds_to_sleep)
        else:
            logger.debug('Required headers for rate throttling are not present in response header, '
                         'unable to throttle ..')

def raise_for_error(response):   
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            error_code = response.status_code

            # Handling status code 429 specially since the required information is present in the headers
            if error_code == 429:
                resp_headers = response.headers
                api_rate_limit_message = ERROR_CODE_EXCEPTION_MAPPING[429]["message"]

                #Raise PipedriveTooManyRequestsInSecondError exception if 2 seconds limit is reached
                if int(resp_headers.get("X-RateLimit-Remaining")) < 1:
                    message = "HTTP-error-code: 429, Error: {} Please retry after {} seconds.".format(api_rate_limit_message, resp_headers.get("X-RateLimit-Reset"))
                    raise PipedriveTooManyRequestsInSecondError(message, response) from None

                message = "HTTP-error-code: 429, Error: Daily {} Please retry after {} seconds.".format(api_rate_limit_message, resp_headers.get("X-RateLimit-Reset"))

            else:
                # Forming a response message for raising custom exception
                try:
                    json_resp = response.json()
                except Exception:
                    json_resp = {}

                message_text = json_resp.get("error", ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error"))
                message = "HTTP-error-code: {}, Error: {}".format(error_code, message_text)

            exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", PipedriveError)
            raise exc(message, response) from None

        except (ValueError, TypeError):
            raise PipedriveError(error) from None

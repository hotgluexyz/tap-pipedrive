#!/usr/bin/env python3

import singer
import json
import sys
from tap_pipedrive.tap import PipedriveTap


logger = singer.get_logger()

@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(['access_token', 'start_date'])

    pipedrive_tap = PipedriveTap(args.config, args.state)

    if args.discover:
        catalog = pipedrive_tap.do_discover(return_dict=True)
        json.dump(catalog, sys.stdout, indent=2)
        logger.info('Finished discover')
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = pipedrive_tap.do_discover()
        pipedrive_tap.do_sync(catalog)


if __name__ == '__main__':
    main()

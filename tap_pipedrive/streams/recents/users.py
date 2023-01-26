from tap_pipedrive.streams.recents import RecentsStream


class RecentUsersStream(RecentsStream):
    items = 'user'
    schema = 'users'
    key_properties = ['id', ]
    state_field = 'modified'
    replication_method = 'INCREMENTAL'

    def process_row(self, row):
        return row['data'][0]

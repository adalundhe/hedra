def to_record(collection_name=None, data=None):
    data_dict = data.to_dict()
    data_dict['tags'] = data.tags.to_dict_list()

    return {
        'type': 'insert',
        'collection': collection_name,
        'documents': [data_dict]
    }
    

def to_query(collection_name=None, key=None, stat_type=None, stat_field=None, partial=False):
    if partial:
        filters = {}
        if key:
            filters[key] = {'not_equals': None}
        if stat_type:
            filters[stat_type] = {'equals': None}
        if stat_field:
            filters[stat_field] = {'equals': None}
        return {
            'type': 'select',
            'collection': collection_name,
            'options': {
                'filter': filters
            }
        }

    else:
        return {
            'type': 'select',
            'collection': collection_name
        }
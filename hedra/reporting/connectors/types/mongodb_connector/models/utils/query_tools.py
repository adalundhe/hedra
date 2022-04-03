import pymongo

def assemble_join(option):
    pipeline_join = {
        '$lookup': {
            'from': option.get('collection'),
            'as': option.get('alias')
        }
    }

    if option.get('pipeline'):
        pipeline_join['$lookup']['pipeline'] = option.get('pipeline')

    elif option.get('function'):
        function = option.get('function', {})

        pipeline_join['$lookup']['let'] = {
            'vars': function.get('vars'),
            'in': function.get('expression')
        }

    else:
        join_fields = {
            'localField': option.get('on_field'),
            'foreignField': option.get(
                'to_field',
                option.get('on_field')
            ),
        }

        pipeline_join['$lookup'] = {
            **pipeline_join.get('$lookup'),
            **join_fields
        }

    return pipeline_join


def assemble_filter(option):
    operators = {
        'equals': '$eq',
        'greater': '$gt',
        'less': '$lt',
        'greater_or_equal': '$gte',
        'less_or_equal': '$lte',
        'not_equals': '$ne'
    }

    filter_dict = {
        "$match": {}
    }

    for field, operator_dict in option.items():
        filter_dict["$match"][field] = {}
        for filter_type, value in operator_dict.items():
            operator = operators.get(filter_type)
            filter_dict["$match"][field][operator] = value
    
    return filter_dict


def assemble_sort(option):
    sort_orders = {
        'ASC': pymongo.ASCENDING,
        'DESC': pymongo.DESCENDING
    }

    return {
        "$sort": {
            field: sort_orders.get(
                order
            ) for field, order in option.items()
        }
    }

def assemble_limit(option):
    return {
        "$limit": option.get('count')
    }


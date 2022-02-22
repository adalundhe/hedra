def parse_statserve_response(summary):
    return {
        
        **summary.get('stats', {}),
        **summary.get('counts', {}),
        **summary.get('quantiles', {})
    }, summary.get('metadata')

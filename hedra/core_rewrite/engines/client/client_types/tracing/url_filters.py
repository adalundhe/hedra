from hedra.core.engines.types.common.url import URL


def default_params_strip_filter(url: URL) -> str:
    return url.parsed._replace(
        query=None
    ).geturl()

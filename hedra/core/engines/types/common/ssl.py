import ssl


def get_http2_ssl_context():
    """
    This function creates an SSLContext object that is suitably configured for
    HTTP/2. If you're working with Python TLS directly, you'll want to do the
    exact same setup as this function does.
    """
    # Get the basic context from the standard library.
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

    # RFC 7540 Section 9.2: Implementations of HTTP/2 MUST use TLS version 1.2
    # or higher. Disable TLS 1.1 and lower.
    ctx.options |= (
        ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
    )

    # RFC 7540 Section 9.2.1: A deployment of HTTP/2 over TLS 1.2 MUST disable
    # compression.
    ctx.options |= ssl.OP_NO_COMPRESSION

    # RFC 7540 Section 9.2.2: "deployments of HTTP/2 that use TLS 1.2 MUST
    # support TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256". In practice, the
    # blocklist defined in this section allows only the AES GCM and ChaCha20
    # cipher suites with ephemeral key negotiation.
    ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20")

    # We want to negotiate using NPN and ALPN. ALPN is mandatory, but NPN may
    # be absent, so allow that. This setup allows for negotiation of HTTP/1.1.
    ctx.set_alpn_protocols(["h2", "http/1.1"])

    try:
        if hasattr(ctx, '_set_npn_protocols'):
            ctx.set_npn_protocols(["h2", "http/1.1"])
    except NotImplementedError:
        pass


    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    return ctx

def get_graphql_ssl_context():
    # Get the basic context from the standard library.
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

    # RFC 7540 Section 9.2: Implementations of HTTP/2 MUST use TLS version 1.2
    # or higher. Disable TLS 1.1 and lower.
    ctx.options |= (
        ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
    )

    # RFC 7540 Section 9.2.1: A deployment of HTTP/2 over TLS 1.2 MUST disable
    # compression.
    ctx.options |= ssl.OP_NO_COMPRESSION

    ctx.minimum_version = ssl.PROTOCOL_TLSv1_2

    # # RFC 7540 Section 9.2.2: "deployments of HTTP/2 that use TLS 1.2 MUST
    # # support TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256". In practice, the
    # # blocklist defined in this section allows only the AES GCM and ChaCha20
    # # cipher suites with ephemeral key negotiation.
    ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20")

    # # # We want to negotiate using NPN and ALPN. ALPN is mandatory, but NPN may
    # # # be absent, so allow that. This setup allows for negotiation of HTTP/1.1.
    ctx.set_alpn_protocols(["h2", "http/1.1"])

    try:
        if hasattr(ctx, '_set_npn_protocols'):
            ctx.set_npn_protocols(["h2", "http/1.1"])
    except NotImplementedError:
        pass

    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    return ctx



def get_default_ssl_context():
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    return ctx

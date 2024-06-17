# Patch: patching of the Python stadard library's ssl module for transparent
# use of datagram sockets.

# Copyright 2012 Ray Brown
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# The License is also distributed with this work in the file named "LICENSE."
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Patch

This module is used to patch the Python standard library's ssl module. Patching
has the following effects:

    * The constant PROTOCOL_DTLSv1 is added at ssl module level
    * DTLSv1's protocol name is added to the ssl module's id-to-name dictionary
    * The constants DTLS_OPENSSL_VERSION* are added at the ssl module level
    * Instantiation of ssl.SSLSocket with sock.type == socket.SOCK_DGRAM is
      supported and leads to substitution of this module's DTLS code paths for
      that SSLSocket instance
    * Direct instantiation of SSLSocket as well as instantiation through
      ssl.wrap_socket are supported
    * Invocation of the function get_server_certificate with a value of
      PROTOCOL_DTLSv1 for the parameter ssl_version is supported
"""

import errno
from socket import AF_INET, SOCK_DGRAM, SOCK_STREAM, getaddrinfo, socket
from socket import error as socket_error
from ssl import CERT_NONE, PROTOCOL_SSLv23
from types import MethodType
from weakref import proxy

from .err import patch_ssl_errors, raise_as_ssl_module_error
from .sslconnection import (
    DTLS_OPENSSL_VERSION,
    DTLS_OPENSSL_VERSION_INFO,
    DTLS_OPENSSL_VERSION_NUMBER,
    PROTOCOL_DTLS,
    SSL_BUILD_CHAIN_FLAG_CHECK,
    SSL_BUILD_CHAIN_FLAG_CLEAR_ERROR,
    SSL_BUILD_CHAIN_FLAG_IGNORE_ERROR,
    SSL_BUILD_CHAIN_FLAG_NO_ROOT,
    SSL_BUILD_CHAIN_FLAG_NONE,
    SSL_BUILD_CHAIN_FLAG_UNTRUSTED,
    PROTOCOL_DTLSv1,
    PROTOCOL_DTLSv1_2,
    SSLConnection,
)


def do_patch():
    import ssl as _ssl  # import to be avoided if ssl module is never patched

    global \
        _orig_SSLSocket_init, \
        _orig_get_server_certificate, \
        _orig_SSLSocket_close, \
        _orig_SSLSocket_settimeout, \
        _orig_SSLSocket___del__
    global ssl
    ssl = _ssl
    if hasattr(ssl, "PROTOCOL_DTLSv1"):
        return
    _orig_wrap_socket = ssl.SSLContext().wrap_socket
    ssl.wrap_socket = _wrap_socket
    ssl.PROTOCOL_DTLS = PROTOCOL_DTLS
    ssl.PROTOCOL_DTLSv1 = PROTOCOL_DTLSv1
    ssl.PROTOCOL_DTLSv1_2 = PROTOCOL_DTLSv1_2
    ssl._PROTOCOL_NAMES[PROTOCOL_DTLS] = "DTLS"
    ssl._PROTOCOL_NAMES[PROTOCOL_DTLSv1] = "DTLSv1"
    ssl._PROTOCOL_NAMES[PROTOCOL_DTLSv1_2] = "DTLSv1.2"
    ssl.DTLS_OPENSSL_VERSION_NUMBER = DTLS_OPENSSL_VERSION_NUMBER
    ssl.DTLS_OPENSSL_VERSION = DTLS_OPENSSL_VERSION
    ssl.DTLS_OPENSSL_VERSION_INFO = DTLS_OPENSSL_VERSION_INFO
    ssl.SSL_BUILD_CHAIN_FLAG_NONE = SSL_BUILD_CHAIN_FLAG_NONE
    ssl.SSL_BUILD_CHAIN_FLAG_UNTRUSTED = SSL_BUILD_CHAIN_FLAG_UNTRUSTED
    ssl.SSL_BUILD_CHAIN_FLAG_NO_ROOT = SSL_BUILD_CHAIN_FLAG_NO_ROOT
    ssl.SSL_BUILD_CHAIN_FLAG_CHECK = SSL_BUILD_CHAIN_FLAG_CHECK
    ssl.SSL_BUILD_CHAIN_FLAG_IGNORE_ERROR = SSL_BUILD_CHAIN_FLAG_IGNORE_ERROR
    ssl.SSL_BUILD_CHAIN_FLAG_CLEAR_ERROR = SSL_BUILD_CHAIN_FLAG_CLEAR_ERROR
    _orig_SSLSocket_init = ssl.SSLSocket.__init__
    _orig_get_server_certificate = ssl.get_server_certificate
    _orig_SSLSocket_close = ssl.SSLSocket.close
    _orig_SSLSocket_settimeout = ssl.SSLSocket.settimeout
    _orig_SSLSocket___del__ = ssl.SSLSocket.__del__
    ssl.SSLSocket.__init__ = _SSLSocket_init
    ssl.get_server_certificate = _get_server_certificate
    ssl.SSLSocket.close = _SSLSocket_close
    ssl.SSLSocket.settimeout = _SSLSocket_settimeout
    ssl.SSLSocket.__del__ = _SSLSocket___del__
    patch_ssl_errors()
    raise_as_ssl_module_error()


def _wrap_socket(
    sock,
    keyfile=None,
    certfile=None,
    server_side=False,
    cert_reqs=CERT_NONE,
    ssl_version=PROTOCOL_DTLS,
    ca_certs=None,
    do_handshake_on_connect=True,
    suppress_ragged_eofs=True,
    ciphers=None,
    cb_user_config_ssl_ctx=None,
    cb_user_config_ssl=None,
):
    return ssl.SSLSocket(
        sock,
        keyfile=keyfile,
        certfile=certfile,
        server_side=server_side,
        cert_reqs=cert_reqs,
        ssl_version=ssl_version,
        ca_certs=ca_certs,
        do_handshake_on_connect=do_handshake_on_connect,
        suppress_ragged_eofs=suppress_ragged_eofs,
        ciphers=ciphers,
        cb_user_config_ssl_ctx=cb_user_config_ssl_ctx,
        cb_user_config_ssl=cb_user_config_ssl,
    )


def _get_server_certificate(addr, ssl_version=PROTOCOL_SSLv23, ca_certs=None):
    """Retrieve a server certificate

    Retrieve the certificate from the server at the specified address,
    and return it as a PEM-encoded string.
    If 'ca_certs' is specified, validate the server cert against it.
    If 'ssl_version' is specified, use it in the connection attempt.
    """

    if ssl_version not in (PROTOCOL_DTLS, PROTOCOL_DTLSv1, PROTOCOL_DTLSv1_2):
        return _orig_get_server_certificate(addr, ssl_version, ca_certs)

    if ca_certs is not None:
        cert_reqs = ssl.CERT_REQUIRED
    else:
        cert_reqs = ssl.CERT_NONE
    af = getaddrinfo(addr[0], addr[1])[0][0]
    s = ssl.wrap_socket(
        socket(af, SOCK_DGRAM),
        ssl_version=ssl_version,
        cert_reqs=cert_reqs,
        ca_certs=ca_certs,
    )
    s.connect(addr)
    dercert = s.getpeercert(True)
    # s = s.unwrap()
    s.close()
    return ssl.DER_cert_to_PEM_cert(dercert)


# _keepalives = []


def _sockclone_kwargs(old):
    """Replace socket(_sock=old._sock) with socket(**_sockclone_kwargs(old))"""
    # _keepalives.append(old) # old socket would be gc'd and implicitly closed otherwise
    return dict(family=old.family, type=old.type, proto=old.proto, fileno=old.fileno())


def _SSLSocket_init(
    self,
    sock=None,
    keyfile=None,
    certfile=None,
    server_side=False,
    cert_reqs=CERT_NONE,
    ssl_version=PROTOCOL_DTLS,
    ca_certs=None,
    do_handshake_on_connect=True,
    family=AF_INET,
    type=SOCK_STREAM,
    proto=0,
    fileno=None,
    suppress_ragged_eofs=True,
    npn_protocols=None,
    ciphers=None,
    server_hostname=None,
    _context=None,
    _session=None,
    cb_user_config_ssl_ctx=None,
    cb_user_config_ssl=None,
):
    is_connection = is_datagram = False
    if isinstance(sock, SSLConnection):
        is_connection = True
    elif hasattr(sock, "type") and (sock.type & SOCK_DGRAM) == SOCK_DGRAM:
        is_datagram = True
    if not is_connection and not is_datagram:
        # Non-DTLS code path
        return _orig_SSLSocket_init(
            self,
            sock=sock,
            keyfile=keyfile,
            certfile=certfile,
            server_side=server_side,
            cert_reqs=cert_reqs,
            ssl_version=ssl_version,
            ca_certs=ca_certs,
            do_handshake_on_connect=do_handshake_on_connect,
            family=family,
            type=type,
            proto=proto,
            fileno=fileno,
            suppress_ragged_eofs=suppress_ragged_eofs,
            npn_protocols=npn_protocols,
            ciphers=ciphers,
            server_hostname=server_hostname,
            _context=_context,
            _session=_session,
        )
    # DTLS code paths: datagram socket and newly accepted DTLS connection
    if is_datagram:
        socket.__init__(self, **_sockclone_kwargs(sock))
    else:
        socket.__init__(self, **_sockclone_kwargs(sock.get_socket(True)))

    # hmm, why?
    if hasattr(sock, "timeout"):
        self.settimeout(sock.timeout)
    if isinstance(sock, socket):
        sock.detach()

    if certfile and not keyfile:
        keyfile = certfile

    class FakeContext(object):
        check_hostname = False

    self._context = FakeContext()
    self.server_side = server_side
    self.keyfile = keyfile
    self.certfile = certfile
    self.cert_reqs = cert_reqs
    self.ssl_version = ssl_version
    self.ca_certs = ca_certs
    self.ciphers = ciphers
    self.do_handshake_on_connect = do_handshake_on_connect
    self.suppress_ragged_eofs = suppress_ragged_eofs
    self._makefile_refs = 0
    self._user_config_ssl_ctx = cb_user_config_ssl_ctx
    self._user_config_ssl = cb_user_config_ssl

    # Perform method substitution and addition (without reference cycle)
    self._real_connect = MethodType(_SSLSocket_real_connect, proxy(self))
    self.listen = MethodType(_SSLSocket_listen, proxy(self))
    self.accept = MethodType(_SSLSocket_accept, proxy(self))
    self.get_timeout = MethodType(_SSLSocket_get_timeout, proxy(self))
    self.handle_timeout = MethodType(_SSLSocket_handle_timeout, proxy(self))

    # Extra
    self.getpeercertchain = MethodType(_getpeercertchain, proxy(self))

    if is_datagram:
        self._connected = False
        # see if it's connected
        try:
            socket.getpeername(self)
        except socket_error as e:
            if e.errno != errno.ENOTCONN:
                raise
            # no, no connection yet
            self._sslobj = None
        else:
            # yes, create the SSL object
            self._sslobj = SSLConnection(
                self,
                keyfile,
                certfile,
                server_side,
                cert_reqs,
                ssl_version,
                ca_certs,
                do_handshake_on_connect,
                suppress_ragged_eofs,
                ciphers,
                cb_user_config_ssl_ctx=cb_user_config_ssl_ctx,
                cb_user_config_ssl=cb_user_config_ssl,
            )
            self._connected = True
    else:
        self._connected = True
        self._sslobj = sock


def _getpeercertchain(self, binary_form=False):
    return self._sslobj.getpeercertchain(binary_form)


def _SSLSocket_listen(self, ignored):
    if self._connected:
        raise ValueError("attempt to listen on connected SSLSocket!")
    if self._sslobj:
        return
    self._sslobj = SSLConnection(
        socket(**_sockclone_kwargs(self)),
        self.keyfile,
        self.certfile,
        True,
        self.cert_reqs,
        self.ssl_version,
        self.ca_certs,
        self.do_handshake_on_connect,
        self.suppress_ragged_eofs,
        self.ciphers,
        cb_user_config_ssl_ctx=self._user_config_ssl_ctx,
        cb_user_config_ssl=self._user_config_ssl,
    )
    if hasattr(self, "timeout"):
        try:
            self._sslobj._sock.settimeout(self.timeout)
        except:
            pass


def _SSLSocket_accept(self):
    if self._connected:
        raise ValueError("attempt to accept on connected SSLSocket!")
    if not self._sslobj:
        raise ValueError("attempt to accept on SSLSocket prior to listen!")
    acc_ret = self._sslobj.accept()
    if not acc_ret:
        return
    new_conn, addr = acc_ret
    new_ssl_sock = ssl.SSLSocket(
        new_conn,
        self.keyfile,
        self.certfile,
        True,
        self.cert_reqs,
        self.ssl_version,
        self.ca_certs,
        self.do_handshake_on_connect,
        self.suppress_ragged_eofs,
        self.ciphers,
        cb_user_config_ssl_ctx=self._user_config_ssl_ctx,
        cb_user_config_ssl=self._user_config_ssl,
    )
    return new_ssl_sock, addr


def _SSLSocket_real_connect(self, addr, return_errno):
    if self._connected:
        raise ValueError("attempt to connect already-connected SSLSocket!")
    if self._sslobj:
        raise RuntimeError("Overwriting SSLConnection?")
    self._sslobj = SSLConnection(
        socket(**_sockclone_kwargs(self)),
        self.keyfile,
        self.certfile,
        False,
        self.cert_reqs,
        self.ssl_version,
        self.ca_certs,
        self.do_handshake_on_connect,
        self.suppress_ragged_eofs,
        self.ciphers,
        cb_user_config_ssl_ctx=self._user_config_ssl_ctx,
        cb_user_config_ssl=self._user_config_ssl,
    )
    if hasattr(self, "timeout"):
        try:
            self._sslobj._sock.settimeout(self.timeout)
        except:
            pass

    try:
        self._sslobj.connect(addr)
    except socket_error as e:
        if return_errno:
            return e.errno
        else:
            self._sslobj = None
            raise e
    self._connected = True
    return 0


def _SSLSocket_get_timeout(self):
    return self._sslobj.get_timeout()


def _SSLSocket_handle_timeout(self):
    return self._sslobj.handle_timeout()


def _SSLSocket_close(self):
    try:
        _orig_SSLSocket_close(self)
    except Exception:
        pass


def _SSLSocket_settimeout(self, timeout):
    try:
        self._sslobj._sock.settimeout(timeout)
    except:
        pass
    return _orig_SSLSocket_settimeout(self, timeout)


def _SSLSocket___del__(self):
    _orig_SSLSocket___del__(self)
    try:
        del self._sslobj
    except:
        pass


if __name__ == "__main__":
    do_patch()

import socket
import sys

def bind_tcp_socket(
    host: str,
    port: int
) -> socket.socket:

    family = socket.AF_INET

    if host and ":" in host:
        family = socket.AF_INET6

    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:

        sock.bind((host, port))

    except OSError :
        sys.exit(1)
    
    sock.setblocking(False)
    sock.set_inheritable(True)

    return sock


def bind_udp_socket(
    host: str,
    port: int
) -> socket.socket:

    sock = socket.socket(
        socket.AF_INET, 
        socket.SOCK_DGRAM, 
        socket.IPPROTO_UDP
    )
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:

        sock.bind((host, port))

    except OSError :
        sys.exit(1)
    
    sock.setblocking(False)
    sock.set_inheritable(True)

    return sock
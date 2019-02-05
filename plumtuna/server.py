from contextlib import closing
import socket
import subprocess

DEFAULT_PORT=7364

class PlumtunaServer(object):
    def __init__(self, bind_addr=None, bind_port=None, contact_host=None, contact_port=None):
        http_port = find_free_port()
        if contact_host is None:
            rpc_addr, rpc_port = find_rpc_server_addr_and_port(bind_addr, bind_port)
        else:
            contact_port = contact_port or DEFAULT_PORT
            rpc_addr, rpc_port = find_rpc_client_addr_and_port(bind_port, bind_port, contact_host, contact_port)

        args = ["plumtuna", "--http_port", str(http_port), "--rpc_addr", "{}:{}".format(rpc_addr, rpc_port)]
        if contact_host is not None:
            args.extend(["--contact_server", "{}:{}".format(contact_host, contact_port)])

        self._process = subprocess.Popen(args, stdin=subprocess.PIPE)
        assert self._process is not None

        self.http_port = http_port
        self.rpc_addr = rpc_addr
        self.rpc_port = rpc_port

    def __del__(self):
        if self._process is not None:
            try:
                self._process.kill()
            except AttributeError:
                pass


def find_rpc_client_addr_and_port(addr=None, port=None, contact_host=None, contact_port=None):
    if addr is None or port is None:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((contact_host, contact_port))
        local_addr, local_port = client.getsockname()
    if addr is None:
        addr = local_addr
    if port is None:
        port = local_port
    return (addr, port)

def find_rpc_server_addr_and_port(addr=None, port=None):
    port = port or find_free_port()
    if addr is None:
        addr = socket.gethostbyname(socket.gethostname())
    else:
        addr = socket.gethostbyname(addr)
    return (addr, port)

def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

def find_local_addr(peer):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((peer_host, peer_port))

import secrets
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from hedra.distributed.env import Env


class AESGCMFernet:

    def __init__(self, env: Env) -> None:
        self.secret = env.MERCURY_SYNC_AUTH_SECRET

    def encrypt(self, data: bytes) -> bytes:
        key = secrets.token_bytes(32)
        nonce = secrets.token_bytes(12)
        return key + nonce + AESGCM(key).encrypt(nonce, data, b"")
    
    def decrypt(self, data: bytes) -> bytes:
        key = data[:32]
        nonce = data[32:44]
        return AESGCM(key).decrypt(nonce, data[44:], b"")
    

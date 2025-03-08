from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

def encrypt(key: bytes, data: bytes, info: bytes) -> bytes:
    """Encrypt data using AES-256-GCM."""
    cipher = Cipher(
        algorithms.AES(key),
        modes.GCM(info),
        backend=default_backend()
    )
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(data) + encryptor.finalize()
    return ciphertext

def decrypt(key: bytes, data: bytes, info: bytes) -> bytes:
    """Decrypt data using AES-256-GCM."""
    cipher = Cipher(
        algorithms.AES(key),
        modes.GCM(info),
        backend=default_backend()
    )
    decryptor = cipher.decryptor()
    plaintext = decryptor.update(data) + decryptor.finalize()
    return plaintext 
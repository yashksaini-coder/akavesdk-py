import os
import hashlib
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

KEY_LENGTH = 32


def derive_key(key: bytes, info: bytes) -> bytes:
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=KEY_LENGTH,
        salt=None,
        info=info,
        backend=default_backend(),
    )
    return hkdf.derive(key)


def make_gcm_cipher(origin_key: bytes, info: bytes):
    key = derive_key(origin_key, info)
    cipher = Cipher(algorithms.AES(key), modes.GCM(os.urandom(12)), backend=default_backend())
    return cipher


def encrypt(key: bytes, data: bytes, info: bytes) -> bytes:
    cipher = make_gcm_cipher(key, info)
    encryptor = cipher.encryptor()
    nonce = encryptor._ctx._nonce  # Get the automatically generated nonce
    ciphertext = encryptor.update(data) + encryptor.finalize()
    return nonce + ciphertext + encryptor.tag


def decrypt(key: bytes, encrypted_data: bytes, info: bytes) -> bytes:
    nonce_size = 12  # AES-GCM standard nonce size
    tag_size = 16  # AES-GCM standard tag size
    if len(encrypted_data) < nonce_size + tag_size:
        raise ValueError("Invalid encrypted data")
    
    nonce = encrypted_data[:nonce_size]
    ciphertext = encrypted_data[nonce_size:-tag_size]
    tag = encrypted_data[-tag_size:]
    
    key = derive_key(key, info)
    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce, tag), backend=default_backend())
    decryptor = cipher.decryptor()
    return decryptor.update(ciphertext) + decryptor.finalize()

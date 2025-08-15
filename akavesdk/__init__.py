import sys
import os

# Add private directory to path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PRIVATE_PATH = os.path.join(PROJECT_ROOT, "private")
if PRIVATE_PATH not in sys.path:
    sys.path.append(PRIVATE_PATH)

# Import and expose main SDK classes
from sdk.sdk import SDK, SDKError, SDKConfig
from sdk.bucket_client import BucketCreateResult, Bucket
from sdk.sdk_streaming import StreamingAPI
from sdk.sdk_ipc import IPC
from sdk.erasure_code import ErasureCode


# Make SDKError appear under akavesdk in tracebacks
SDKError.__module__ = "akavesdk"

# Define what gets imported with "from akavesdk import *"
__all__ = ["SDK", "SDKError", "SDKConfig", "StreamingAPI", "IPC", 
           "BucketCreateResult", "Bucket", "ErasureCode"]
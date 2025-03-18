import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PRIVATE_PATH = os.path.join(PROJECT_ROOT, "private")
if PRIVATE_PATH not in sys.path:
    sys.path.append(PRIVATE_PATH)

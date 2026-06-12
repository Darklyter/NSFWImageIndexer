import os
import sys

# Project root on sys.path so engine modules import the same way the app
# does (namespace package: `from src import llmii`).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

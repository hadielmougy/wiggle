"""Put the package root on sys.path so `import wiggle` works when running pytest from this
directory, both under Gradle and by hand (`pytest` in clients/python)."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

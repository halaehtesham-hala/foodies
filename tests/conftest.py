import os
import sys
import tempfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Use a throw-away database and fast background steps so tests never touch real data.
os.environ["FOODIES_DB"] = os.path.join(tempfile.mkdtemp(), "test.db")
os.environ["FOODIES_STEP_DELAY"] = "0.01"

import app as foodies  # noqa: E402  (must be imported after the environment is set)


@pytest.fixture
def client():
    foodies.app.config["TESTING"] = True
    foodies.run("DELETE FROM cart_items")
    return foodies.app.test_client()


@pytest.fixture
def logged_in(client):
    client.post("/login", data={"email": "demo@foodies.pk", "password": "demo123"})
    return client

"""anthropic SDK를 가짜로 바꾸는 픽스처. 실제 API를 부르지 않고, 예외 문구에 일부러 키를 넣어 새는지 본다."""
import sys
import types

import pytest

FAKE_KEY = "fake-anthropic-key-for-tests-0123456789"


@pytest.fixture
def fake_sdk(monkeypatch):
    module = types.ModuleType("anthropic")

    class APIError(Exception):
        pass

    class APIConnectionError(APIError):
        pass

    class Messages:
        def __init__(self, behaviour):
            self.behaviour = behaviour
            self.requests = []

        def create(self, **kwargs):
            self.requests.append(kwargs)
            return self.behaviour(kwargs)

    class Anthropic:
        def __init__(self, api_key):
            self.api_key = api_key
            self.messages = Messages(module.behaviour)

    def fail(_kwargs):
        raise APIConnectionError(f"connection failed with x-api-key: {FAKE_KEY}")

    module.APIError = APIError
    module.Anthropic = Anthropic
    module.behaviour = fail
    monkeypatch.setitem(sys.modules, "anthropic", module)
    return module

"""판정 워크플로 실행 요청 시험(가짜 HTTP). 실제 GitHub는 부르지 않는다."""
import json
import logging
import urllib.error

from ops_checker.config import Settings
from ops_checker.dispatch import GithubDispatch

TOKEN = "fake-token-value"


class FakeResponse:
    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class FakeOpener:
    def __init__(self, status: int = 204, error: Exception | None = None) -> None:
        self.status, self.error, self.requests = status, error, []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        if self.error:
            raise self.error
        return FakeResponse(self.status)


def test_requests_the_verdict_workflow_on_the_given_branch():
    opener = FakeOpener()
    assert GithubDispatch(TOKEN, "owner/repo", "develop", opener).fire() is True
    request, timeout = opener.requests[0]
    assert request.get_method() == "POST"
    assert request.full_url == "https://api.github.com/repos/owner/repo/actions/workflows/ops-verdict.yml/dispatches"
    assert json.loads(request.data) == {"ref": "develop"}
    assert request.get_header("Authorization") == f"Bearer {TOKEN}"
    assert timeout > 0


def test_does_not_call_when_token_or_repo_is_empty():
    opener = FakeOpener()
    assert GithubDispatch("", "owner/repo", "develop", opener).fire() is False
    assert GithubDispatch(TOKEN, "", "develop", opener).fire() is False
    assert opener.requests == []


def test_failures_return_false_without_raising_or_logging_the_token(caplog):
    caplog.set_level(logging.INFO)
    http = urllib.error.HTTPError("u", 401, "Bad credentials", {}, None)
    for opener in (FakeOpener(error=http), FakeOpener(error=urllib.error.URLError("down")),
                   FakeOpener(error=TimeoutError()), FakeOpener(status=200)):
        assert GithubDispatch(TOKEN, "owner/repo", "develop", opener).fire() is False
    assert TOKEN not in caplog.text
    assert "http 401" in caplog.text


def test_settings_read_the_dispatch_values_and_hide_the_token():
    env = {"OPS_CHECKER_DB_PASSWORD": "pw", "OPS_RESULTS_BUCKET": "bucket",
           "OPS_GITHUB_TOKEN": TOKEN, "OPS_VERDICT_REPO": "owner/repo"}
    settings = Settings.from_env(env)
    assert (settings.github_token, settings.verdict_repo, settings.verdict_ref) == (TOKEN, "owner/repo", "develop")
    assert TOKEN not in repr(settings)
    empty = Settings.from_env({"OPS_CHECKER_DB_PASSWORD": "pw", "OPS_RESULTS_BUCKET": "bucket"})
    assert (empty.github_token, empty.verdict_repo) == ("", "")


def test_bad_repo_name_is_a_failure_not_an_exception(caplog):
    # 기본 연결기로 실제 요청을 만들되, 주소가 잘못돼 나가기 전에 실패한다.
    assert GithubDispatch(TOKEN, "owner/re po", "develop").fire() is False
    assert TOKEN not in caplog.text


def test_redirect_is_not_followed_so_the_token_stays_put():
    # 로컬 서버 두 개: 첫 서버는 다른 서버로 리다이렉트하고, 두 번째 서버는 요청이 오면 기록한다.
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    seen = []

    class Other(BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append(self.headers.get("Authorization"))
            self.send_response(204)
            self.end_headers()

        do_POST = do_GET

        def log_message(self, *_):
            pass

    other = HTTPServer(("127.0.0.1", 0), Other)

    class Redirect(BaseHTTPRequestHandler):
        def do_POST(self):
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{other.server_port}/steal")
            self.end_headers()

        def log_message(self, *_):
            pass

    first = HTTPServer(("127.0.0.1", 0), Redirect)
    for server in (first, other):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        dispatch = GithubDispatch(TOKEN, "owner/repo", "develop", api=f"http://127.0.0.1:{first.server_port}")
        assert dispatch.fire() is False
        assert seen == []
    finally:
        for server in (first, other):
            server.shutdown()
            server.server_close()

"""결과 파일을 올린 뒤 판정 워크플로를 바로 실행해 달라고 GitHub에 요청한다(workflow_dispatch).
GitHub 예약 실행은 몇 시간씩 늦거나 빠지므로 확인기가 직접 부르고, 예약 실행은 이 요청이 실패한 날의 대비로 남긴다.
요청 실패는 로그만 남기고 확인기 처리를 실패로 만들지 않는다. 토큰 값은 로그에 남기지 않는다."""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Callable, Protocol

log = logging.getLogger("ops-checker")

API = "https://api.github.com"
WORKFLOW = "ops-verdict.yml"
TIMEOUT_SECONDS = 10


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """리다이렉트를 따라가지 않는다. 따라가면 토큰이 붙은 요청이 다른 주소로 갈 수 있고, 이 요청에는 리다이렉트가 필요 없다.
    따라가지 않은 3xx 응답은 HTTPError 로 올라와 실패로 처리된다."""

    def redirect_request(self, *args, **kwargs):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


class VerdictTrigger(Protocol):
    def fire(self) -> bool:
        """판정 실행을 요청한다. 요청이 받아들여졌으면 True. 예외를 던지지 않는다."""


class GithubDispatch:
    def __init__(self, token: str, repo: str, ref: str,
                 opener: Callable[..., object] = _OPENER.open, api: str = API) -> None:
        self._token = token
        self._repo = repo
        self._ref = ref
        self._opener = opener
        self._api = api

    def fire(self) -> bool:
        if not self._token or not self._repo:
            # 토큰·저장소를 넣기 전에 배포돼도 돌도록, 비어 있으면 요청하지 않고 예약 실행에 맡긴다.
            log.info("verdict dispatch off (token or repo not set)")
            return False
        try:
            request = urllib.request.Request(
                f"{self._api}/repos/{self._repo}/actions/workflows/{WORKFLOW}/dispatches",
                data=json.dumps({"ref": self._ref}).encode("utf-8"), method="POST",
                headers={"Authorization": f"Bearer {self._token}", "Accept": "application/vnd.github+json",
                         "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "ops-checker"})
            with self._opener(request, timeout=TIMEOUT_SECONDS) as response:
                status = response.status
        except urllib.error.HTTPError as error:
            log.warning("verdict dispatch failed: http %d", error.code)
            return False
        except Exception as error:  # noqa: BLE001 - 잘못된 저장소 이름 등 무엇이든 확인기 처리를 멈추지 않고 종류만 남긴다
            log.warning("verdict dispatch failed: %s", type(error).__name__)
            return False
        if status != 204:
            log.warning("verdict dispatch unexpected status %d", status)
            return False
        log.info("verdict dispatch requested")
        return True


class NoTrigger:
    """판정 요청을 하지 않는다(시험과 기본값)."""

    def fire(self) -> bool:
        return False

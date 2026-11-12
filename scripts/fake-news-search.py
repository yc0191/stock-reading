#!/usr/bin/env python3
"""고정 입력 시험용 가짜 뉴스 검색 서버. 어떤 검색어를 받아도 같은 구성의 응답을 돌려준다.

검색어마다 20건을 돌려주며 구성은 항상 같다(고정 입력 시험용).
- 필터를 통과하고 서로 다른 기사: 14건
- 같은 기사의 주소 변형(www. 붙임, utm 쿼리): 2건 (정규화하면 중복)
- 24시간이 지난 기사: 1건, 1시간 넘게 미래인 기사: 1건
- 검색어와 무관한 기사: 2건

사용법: python3 scripts/fake-news-search.py [포트, 기본 58080]
호출 수는 http://127.0.0.1:<포트>/__stats, 각 호출을 받은 시각(밀리초)은 /__calls 로 확인한다.
로컬 루프백에만 연다.
"""
import datetime as dt
import hashlib
import json
import sys
import time
import urllib.parse
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

PASSING = 14
calls = {"count": 0}
# 호출을 받은 시각. 틱의 처리 시간을 호스트 시계 하나로 재기 위해 남긴다.
call_times = []


def pub_date(hours_from_now):
    return format_datetime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=hours_from_now))


def item(title, url, hours_from_now):
    return {"title": title, "originallink": url, "link": "https://provider.example.com/r",
            "description": "요약 본문", "pubDate": pub_date(hours_from_now)}


def items_for(query):
    # 주소는 검색어에서 결정되므로 같은 검색어는 언제나 같은 기사를 돌려준다.
    slug = hashlib.sha1(query.encode("utf-8")).hexdigest()[:10]
    base = f"fake-news.example.com/{slug}"
    result = [item(f"{query} 관련 소식 {k}", f"https://{base}/{k}", -(0.5 * (k + 1))) for k in range(PASSING)]
    result.append(item(f"{query} 관련 소식 0", f"https://www.{base}/0", -0.5))
    result.append(item(f"{query} 관련 소식 1", f"https://{base}/1?utm_source=test", -1.0))
    result.append(item(f"{query} 오래된 소식", f"https://{base}/old", -30))
    result.append(item(f"{query} 미래 소식", f"https://{base}/future", 3))
    result.append(item("무관한 소식 1", f"https://{base}/other/1", -1))
    result.append(item("무관한 소식 2", f"https://{base}/other/2", -1))
    return result


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/__stats":
            body = json.dumps(calls).encode()
        elif parsed.path == "/__calls":
            body = json.dumps(call_times).encode()
        else:
            query = urllib.parse.parse_qs(parsed.query).get("query", [""])[0]
            calls["count"] += 1
            call_times.append(int(time.time() * 1000))
            body = json.dumps({"items": items_for(query)}, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 58080
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()

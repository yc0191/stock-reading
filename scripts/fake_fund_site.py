#!/usr/bin/env python3
"""구성 종목 리허설용 가짜 운용사 서버. 출처 번호(fundId)마다 정해 둔 응답을 돌려준다.

- RHRS-OK: 200, 주식 두 종목(미국 티커 하나, 국내 코드 하나)과 현금 행. 원문 앞뒤 공백과 줄바꿈까지 그대로 보낸다.
- RHRS-DOWN: 503, 본문 "unavailable"
- 그 밖: 404, 빈 본문

요청마다 받은 경로를 한 줄씩 요청 기록 파일에 남긴다(ETF마다 요청이 한 번인지 리허설 확인이 센다).
사용법: python3 scripts/fake_fund_site.py <포트> <요청 기록 파일>
로컬 루프백에만 연다.
"""
import sys
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

OK_FUND = "RHRS-OK"
DOWN_FUND = "RHRS-DOWN"
OK_BODY = (' {"pdf": {"list": ['
           '{"secNm": "REHEARSAL A", "itmNo": "RHRSA US Equity", "ratio": " 9.50 "}, '
           '{"secNm": "REHEARSAL KR", "itmNo": "9RHRS1", "ratio": "4.25"}, '
           '{"secNm": "CASH", "itmNo": "CASH", "ratio": null}]}}\n')
DOWN_BODY = "unavailable"
# 위 원문에서 수집기가 골라야 하는 상위 구성(시장, 종목코드, 순위)
OK_MEMBERS = [("US", "RHRSA", 1), ("KR", "9RHRS1", 2)]


def response_for(fund_id):
    if fund_id == OK_FUND:
        return 200, OK_BODY
    if fund_id == DOWN_FUND:
        return 503, DOWN_BODY
    return 404, ""


def main(argv):
    port, request_log = int(argv[1]), argv[2]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            fund_id = path.rsplit("/", 1)[-1]
            with open(request_log, "a", encoding="utf-8") as f:
                f.write(fund_id + "\n")
            status, text = response_for(fund_id)
            body = text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    HTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main(sys.argv)

"""README 구성도(docs/images/architecture-light.svg, -dark.svg)를 그린다. 실행: python3 docs/images/architecture.py docs/images

서비스마다 하는 일, Kafka를 거쳐 서비스에서 서비스로 가는 핵심 이벤트, 브리핑이 근거를 묻는 내부 조회(HTTP)를 보여 준다
(재검색, 수동 명령, 실패 알림 등은 뺌). 브리핑을 시세처리기와 뉴스처리기 사이에 두어 내부 조회 선이 Kafka 허브를 지나지 않게 한다.
이벤트는 한글 이름으로 적고, 토픽 이름은 README의 순서도에 둔다. 이벤트 목록의 원본은 각 서비스의 Topics와 브리핑 main.py다.
"""
import sys
from xml.sax.saxutils import escape

THEMES = {
    "light": dict(bg="#ffffff", text="#1f2328", sub="#59636e", svc="#eef4ff", svcline="#3b6fd4",
                  ext="#ffffff", extline="#8c959f", bus="#fff4e5", busline="#d9822b", line="#8c959f",
                  red="#cf222e", blue="#0969da", gray="#59636e", purple="#8250df", chip="#ffffff",
                  db="#f1f8f4", dbline="#2f8f5b"),
    "dark": dict(bg="#0d1117", text="#e6edf3", sub="#9198a1", svc="#132339", svcline="#4c8eda",
                 ext="#0d1117", extline="#656c76", bus="#2b1d0e", busline="#d9822b", line="#656c76",
                 red="#ff7b72", blue="#58a6ff", gray="#9198a1", purple="#d2a8ff", chip="#0d1117",
                 db="#0f2419", dbline="#3fb970"),
}

SERVICES = {
    "collector": dict(title="수집기", lang="Java", logic=[
        "타이머: 1초 시세, 주기마다 뉴스·공시·지수·환율",
        "평일 아침 ETF 구성 종목 조회",
        "마감 후 실패분 재시도 → 판 시작 알림"]),
    "briefing": dict(title="브리핑", lang="Python", logic=[
        "급변동 한 건 / 하루 두 번 판마다 작업",
        "LLM 문장의 근거 번호를 코드가 검사",
        "근거 약한 문장만 다시 검색해 고쳐 씀",
        "보내기 전 전체 검사, 단계별 실행 기록"]),
    "market": dict(title="시세처리기", lang="Java", logic=[
        "시세·지수·환율·구성 종목 저장",
        "급변동 감지: 연속 확인, 재발행 간격",
        "조회 API: 시세 이력·배경 자료"]),
    "news": dict(title="뉴스처리기", lang="Java", logic=[
        "기사·공시 저장(중복 제거)",
        "조회 API: 시각 범위 안 기사·공시",
        "ETF로 꺼내면 구성 종목 기사까지"]),
    "notifier": dict(title="발송기", lang="Java", logic=[
        "멤버 수신 설정으로 받는 사람 고름",
        "글자판 + HTML·그림 메일",
        "기본 드라이런, 주소 마스킹"]),
}


def chip_width(name):
    return sum(12.5 if ord(ch) > 127 else 7.0 for ch in name) + 22


def svg(theme):
    c = THEMES[theme]
    W, H = 1300, 770
    out = []
    a = out.append
    a(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
      f'font-family="-apple-system, \'Segoe UI\', \'Apple SD Gothic Neo\', \'Noto Sans KR\', sans-serif">')
    a(f'<rect width="{W}" height="{H}" fill="{c["bg"]}"/>')
    col = {"r": c["red"], "b": c["blue"], "g": c["gray"], "p": c["purple"]}
    a('<defs>')
    for k, v in list(col.items()) + [("l", c["line"])]:
        a(f'<marker id="m{k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
          f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{v}"/></marker>')
    a('</defs>')

    def text(x, y, s, size=13, weight=400, color=None, anchor="start", family=None):
        f = f' font-family="{family}"' if family else ''
        a(f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-size="{size}" font-weight="{weight}" '
          f'fill="{color or c["text"]}"{f}>{escape(s)}</text>')

    def path(points, kind="l", dash=False, head=True, both=False):
        d = " ".join(f"{x},{y}" for x, y in points)
        stroke = c["line"] if kind == "l" else col[kind]
        da = ' stroke-dasharray="6 4"' if dash else ''
        m = f' marker-end="url(#m{kind})"' if head else ''
        if both:
            m += f' marker-start="url(#m{kind})"'
        a(f'<polyline points="{d}" fill="none" stroke="{stroke}" stroke-width="{1.6 if kind == "l" else 2}" '
          f'stroke-linejoin="round"{da}{m}/>')

    def chip(cx, cy, name, kind):
        w = chip_width(name)
        a(f'<rect x="{cx - w / 2}" y="{cy - 11}" width="{w}" height="22" rx="11" fill="{c["chip"]}" stroke="{col[kind]}" stroke-width="1.5"/>')
        text(cx, cy + 4.5, name, 12.5, 600, color=col[kind], anchor="middle")

    def ext(x, y, w, h, title, sub=None):
        a(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{c["ext"]}" stroke="{c["extline"]}" '
          f'stroke-width="1.5" stroke-dasharray="5 4"/>')
        text(x + w / 2, y + h / 2 + (-3 if sub else 5), title, 14, 600, anchor="middle")
        if sub:
            text(x + w / 2, y + h / 2 + 15, sub, 11.5, color=c["sub"], anchor="middle")

    def service(key, x, y, w, h):
        s = SERVICES[key]
        a(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{c["svc"]}" stroke="{c["svcline"]}" stroke-width="1.6"/>')
        text(x + 16, y + 28, s["title"], 17, 700)
        text(x + 16 + len(s["title"]) * 17 + 8, y + 28, s["lang"], 12, color=c["sub"])
        for i, line in enumerate(s["logic"]):
            text(x + 16, y + 54 + i * 21, "· " + line, 13)

    # 위: 수집기·발송기, 가운데: Kafka, 아래: 시세처리기·브리핑·뉴스처리기
    TOP, TOP_H = 44, 128
    BUS, BUS_H = 212, 176
    LOW, LOW_H = 428, 128
    service("collector", 172, TOP, 430, TOP_H)
    service("notifier", 640, TOP, 320, TOP_H)
    service("market", 24, LOW, 370, LOW_H)
    service("briefing", 454, LOW, 402, LOW_H)
    service("news", 916, LOW, 360, LOW_H)

    a(f'<rect x="24" y="{BUS}" width="1252" height="{BUS_H}" rx="16" fill="{c["bus"]}" stroke="{c["busline"]}" stroke-width="2"/>')
    text(44, BUS + 30, "Kafka", 18, 700)
    text(44, BUS + 50, "이벤트 허브", 12, color=c["sub"])

    top_b, low_t = TOP + TOP_H, LOW
    lane1, lane2, lane3, lane4 = BUS + 40, BUS + 80, BUS + 118, BUS + 154
    # 수집기 → 시세처리기
    path([(250, top_b), (250, low_t)], "r")
    chip(250, lane1, "시세·지수·환율", "r")
    path([(350, top_b), (350, low_t)], "g")
    chip(350, lane3, "구성 종목", "g")
    # 수집기 → 브리핑: 판 자료 수집과 재시도가 끝났다는 알림. 브리핑은 이 알림으로 데일리 판을 시작한다.
    path([(540, top_b), (540, low_t)], "b")
    chip(540, lane2, "판 자료 수집 끝", "b")
    # 시세처리기 → 브리핑 (아래 → 허브 → 아래)
    path([(374, low_t), (374, lane4), (480, lane4), (480, low_t)], "r")
    chip(427, lane4, "급변동", "r")
    # 뉴스처리기 → 브리핑: 기사를 모두 저장했다는 알림. 브리핑은 이 알림 뒤 기사를 한 번만 조회한다.
    path([(960, low_t), (960, lane1), (836, lane1), (836, low_t)], "r")
    chip(960, lane2, "기사 저장 끝", "r")
    # 브리핑 → 발송기
    path([(700, low_t), (700, top_b)], "r")
    chip(700, lane1, "급변동 브리핑", "r")
    path([(790, low_t), (790, top_b)], "b")
    chip(790, lane3, "데일리 판", "b")
    # 수집기 → 뉴스처리기: 위쪽으로 돌아 오른쪽에서 허브를 지난다(다른 선과 겹치지 않게).
    path([(560, TOP), (560, 20), (1200, 20), (1200, low_t)], "g")
    chip(1200, lane2, "수집한 기사", "g")

    # 내부 조회(HTTP): 브리핑이 시세처리기·뉴스처리기에 직접 묻는다. Kafka를 거치지 않는다.
    qy = LOW + 70
    path([(454, qy), (394, qy)], "p", dash=True)
    text(424, qy - 8, "조회", 11.5, 600, color=c["purple"], anchor="middle")
    path([(856, qy), (916, qy)], "p", dash=True)
    text(886, qy - 8, "조회", 11.5, 600, color=c["purple"], anchor="middle")

    # 외부
    ext(24, 64, 128, 88, "외부 API", "시세 · 뉴스 · 공시")
    path([(152, 108), (172, 108)])
    ext(990, 64, 140, 88, "이메일")
    path([(960, 108), (990, 108)])
    LLM = LOW + LOW_H + 22
    ext(700, LLM, 156, 50, "Claude API")
    path([(778, LOW + LOW_H), (778, LLM)], dash=True)

    # 저장소
    DB = LLM + 72
    a(f'<rect x="24" y="{DB}" width="1252" height="52" rx="10" fill="{c["db"]}" stroke="{c["dbline"]}" stroke-width="1.5"/>')
    text(650, DB + 24, "PostgreSQL", 15, 600, anchor="middle")
    text(650, DB + 42, "서비스별 스키마 · 종목·멤버 표는 필요한 서비스(수집기·발송기 포함)만 읽기 전용", 12, color=c["sub"], anchor="middle")
    for x in (210, 560, 1096):
        path([(x, LOW + LOW_H), (x, DB)], head=False)

    # 범례
    ly = DB + 84
    text(24, ly, "실선 = Kafka 이벤트(내보내는 서비스 → 받는 서비스)", 12.5, 600)
    x = 420
    for kind, label, dash in (("r", "급변동 경로", False), ("b", "데일리 판", False), ("g", "수집·저장", False),
                              ("p", "내부 조회(HTTP, Kafka 안 거침)", True)):
        da = ' stroke-dasharray="6 4"' if dash else ''
        a(f'<line x1="{x}" y1="{ly - 4}" x2="{x + 28}" y2="{ly - 4}" stroke="{col[kind]}" stroke-width="2.5"{da}/>')
        text(x + 36, ly, label, 12.5)
        x += 36 + len(label) * 12 + 34
    a('</svg>')
    return "\n".join(out)


if __name__ == "__main__":
    for t in THEMES:
        with open(f"{sys.argv[1]}/architecture-{t}.svg", "w", encoding="utf-8") as f:
            f.write(svg(t))

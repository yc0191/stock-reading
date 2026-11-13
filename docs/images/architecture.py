"""README 구성도(docs/images/architecture-light.svg, -dark.svg)를 그린다. 실행: python3 docs/images/architecture.py docs/images

서비스마다 하는 일과, Kafka를 거쳐 서비스에서 서비스로 가는 핵심 이벤트만 보여 준다(근거 조회 요청·응답, 재검색, 수동 명령 등은 뺌). 이벤트 목록의 원본은 각 서비스의 Topics와 브리핑 main.py다.
"""
import sys
from xml.sax.saxutils import escape

THEMES = {
    "light": dict(bg="#ffffff", text="#1f2328", sub="#59636e", svc="#eef4ff", svcline="#3b6fd4",
                  ext="#ffffff", extline="#8c959f", bus="#fff4e5", busline="#d9822b", line="#8c959f",
                  red="#cf222e", blue="#0969da", gray="#59636e", chip="#ffffff", db="#f1f8f4", dbline="#2f8f5b"),
    "dark": dict(bg="#0d1117", text="#e6edf3", sub="#9198a1", svc="#132339", svcline="#4c8eda",
                 ext="#0d1117", extline="#656c76", bus="#2b1d0e", busline="#d9822b", line="#656c76",
                 red="#ff7b72", blue="#58a6ff", gray="#9198a1", chip="#0d1117", db="#0f2419", dbline="#3fb970"),
}
MONO = "ui-monospace, SFMono-Regular, Menlo, monospace"

SERVICES = {
    "collector": dict(title="수집기", lang="Java", logic=[
        "타이머: 1초 시세, 주기마다 뉴스·공시·지수·환율",
        "시세 응답 칸 검사, 휴장일 판정",
        "판 수집 끝나면 판 시작 알림 발행"]),
    "briefing": dict(title="브리핑", lang="Python", logic=[
        "급변동 한 건 / 하루 두 번 판마다 작업 시작",
        "시세처리기·뉴스처리기에서 범위 안 근거 조회",
        "LLM 문장의 근거 번호를 코드가 검사",
        "근거 약한 문장만 다시 검색해 고쳐 씀"]),
    "market": dict(title="시세처리기", lang="Java", logic=[
        "시세·지수·환율 저장",
        "급변동 감지: 연속 확인, 같은 종목 재발행 간격",
        "ETF 구성 종목 조회와 변경 기록"]),
    "news": dict(title="뉴스처리기", lang="Java", logic=[
        "기사·공시 저장(중복 제거)",
        "발행 시각 범위 안 기사 조회",
        "ETF로 꺼내면 구성 종목 기사까지"]),
    "notifier": dict(title="발송기", lang="Java", logic=[
        "멤버 수신 설정으로 받는 사람 고름",
        "글자판 + HTML·그림 메일",
        "기본 드라이런, 주소 마스킹"]),
}


def chip_width(name):
    return len(name) * 7.0 + 16


# 이벤트 흐름: (내보내는 서비스, 받는 서비스, 이벤트, 색). 원본은 각 서비스의 Topics와 브리핑 main.py다.
FLOWS = [
    ("collector", "news", "news.collected", "g"),
    ("collector", "market", "market.snapshot", "r"),
    ("collector", "briefing", "daily.collection.completed", "b"),
    ("market", "briefing", "market.move", "r"),
    ("briefing", "notifier", "briefing.generated", "r"),
    ("briefing", "notifier", "briefing.daily.generated", "b"),
]


def svg(theme):
    c = THEMES[theme]
    W, H = 1280, 690
    out = []
    a = out.append
    a(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
      f'font-family="-apple-system, \'Segoe UI\', \'Apple SD Gothic Neo\', \'Noto Sans KR\', sans-serif">')
    a(f'<rect width="{W}" height="{H}" fill="{c["bg"]}"/>')
    col = {"r": c["red"], "b": c["blue"], "g": c["gray"]}
    a('<defs>')
    for k, v in list(col.items()) + [("l", c["line"])]:
        a(f'<marker id="m{k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
          f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{v}"/></marker>')
    a('</defs>')

    def text(x, y, s, size=13, weight=400, color=None, anchor="start", family=None):
        f = f' font-family="{family}"' if family else ''
        a(f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-size="{size}" font-weight="{weight}" '
          f'fill="{color or c["text"]}"{f}>{escape(s)}</text>')

    def path(points, kind="l", dash=False, head=True):
        d = " ".join(f"{x},{y}" for x, y in points)
        stroke = c["line"] if kind == "l" else col[kind]
        da = ' stroke-dasharray="5 4"' if dash else ''
        m = f' marker-end="url(#m{kind})"' if head else ''
        a(f'<polyline points="{d}" fill="none" stroke="{stroke}" stroke-width="{1.6 if kind == "l" else 2}" '
          f'stroke-linejoin="round"{da}{m}/>')

    def chip(cx, cy, name, kind):
        w = chip_width(name)
        a(f'<rect x="{cx - w / 2}" y="{cy - 11}" width="{w}" height="22" rx="11" fill="{c["chip"]}" stroke="{col[kind]}" stroke-width="1.5"/>')
        text(cx, cy + 4.5, name, 12, 600, color=col[kind], anchor="middle", family=MONO)

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

    TOP, TOP_H = 40, 140
    BUS, BUS_H = 236, 136
    LOW, LOW_H = 428, 110
    service("collector", 200, TOP, 440, TOP_H)
    service("briefing", 700, TOP, 420, TOP_H)
    service("news", 24, LOW, 400, LOW_H)
    service("market", 444, LOW, 400, LOW_H)
    service("notifier", 864, LOW, 392, LOW_H)

    # Kafka 허브: 서비스 사이 이벤트는 모두 여기를 지난다.
    a(f'<rect x="24" y="{BUS}" width="1232" height="{BUS_H}" rx="16" fill="{c["bus"]}" stroke="{c["busline"]}" stroke-width="2"/>')
    text(44, BUS + 30, "Kafka", 18, 700)
    text(44, BUS + 50, "이벤트 허브", 12, color=c["sub"])

    top_b, low_t = TOP + TOP_H, LOW
    mid1, mid2 = BUS + 46, BUS + 96
    # 위 → 아래 (곧은 화살표)
    path([(300, top_b), (300, low_t)], "g")
    chip(300, mid1, "news.collected", "g")
    path([(560, top_b), (560, low_t)], "r")
    chip(560, mid1, "market.snapshot", "r")
    # 수집기 → 브리핑 (허브 안에서 돌아 올라감)
    path([(628, top_b), (628, mid2), (740, mid2), (740, top_b)], "b")
    chip(684, mid2, "daily.collection.completed", "b")
    # 시세처리기 → 브리핑 (아래 → 위)
    path([(800, low_t), (800, top_b)], "r")
    chip(800, mid1, "market.move", "r")
    # 브리핑 → 발송기
    path([(960, top_b), (960, low_t)], "r")
    chip(960, mid1, "briefing.generated", "r")
    path([(1090, top_b), (1090, low_t)], "b")
    chip(1090, mid2, "briefing.daily.generated", "b")

    # 외부
    ext(24, 62, 150, 96, "외부 API", "시세 · 뉴스 · 공시")
    path([(174, 110), (200, 110)])
    ext(1146, 62, 110, 96, "Claude", "API")
    path([(1120, 110), (1146, 110)], dash=True)

    # 저장소와 이메일
    DB = 582
    a(f'<rect x="24" y="{DB}" width="1040" height="52" rx="10" fill="{c["db"]}" stroke="{c["dbline"]}" stroke-width="1.5"/>')
    text(544, DB + 24, "PostgreSQL", 15, 600, anchor="middle")
    text(544, DB + 42, "서비스별 스키마 · 종목·멤버 표는 필요한 서비스만 읽기 전용", 12, color=c["sub"], anchor="middle")
    for x in (224, 644, 960):
        path([(x, LOW + LOW_H), (x, DB)], head=False)
    ext(1084, DB, 172, 52, "이메일")
    path([(1170, LOW + LOW_H), (1170, DB)])

    # 범례
    ly = 672
    text(24, ly, "화살표 = 이벤트를 내보내는 서비스 → 받는 서비스 (모두 Kafka를 거침)", 12.5, 600)
    x = 560
    for kind, label in (("r", "급변동 한 건의 흐름"), ("b", "데일리 판 흐름"), ("g", "기사 수집")):
        a(f'<line x1="{x}" y1="{ly - 4}" x2="{x + 28}" y2="{ly - 4}" stroke="{col[kind]}" stroke-width="2.5"/>')
        text(x + 36, ly, label, 12.5)
        x += 36 + len(label) * 13 + 30
    a('</svg>')
    return "\n".join(out)


if __name__ == "__main__":
    for t in THEMES:
        with open(f"{sys.argv[1]}/architecture-{t}.svg", "w", encoding="utf-8") as f:
            f.write(svg(t))

"""README 배포 구성도(docs/images/deploy-light.svg, -dark.svg)를 그린다. 실행: python3 docs/images/deploy.py docs/images

ADR-013의 결정(서버 2대, Compose, 서버가 ECR 이미지를 받아 옴, 들어오는 연결 없음)과 infra/deploy/README.md의 타이머만 보여 준다.
원본은 infra/terraform/, infra/deploy/, .github/workflows/다.
"""
import sys
from xml.sax.saxutils import escape

from architecture import THEMES


def svg(theme):
    c = THEMES[theme]
    W, H = 1300, 560
    out = []
    a = out.append
    a(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
      f'font-family="-apple-system, \'Segoe UI\', \'Apple SD Gothic Neo\', \'Noto Sans KR\', sans-serif">')
    a(f'<rect width="{W}" height="{H}" fill="{c["bg"]}"/>')
    a('<defs>')
    for k, v in (("l", c["line"]), ("b", c["blue"]), ("p", c["purple"])):
        a(f'<marker id="d{k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
          f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{v}"/></marker>')
    a('</defs>')

    def text(x, y, s, size=13, weight=400, color=None, anchor="start"):
        a(f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-size="{size}" font-weight="{weight}" '
          f'fill="{color or c["text"]}">{escape(s)}</text>')

    def path(points, kind="l", dash=False, both=False):
        stroke = {"l": c["line"], "b": c["blue"], "p": c["purple"]}[kind]
        d = " ".join(f"{x},{y}" for x, y in points)
        da = ' stroke-dasharray="6 4"' if dash else ''
        m = f' marker-end="url(#d{kind})"' + (f' marker-start="url(#d{kind})"' if both else '')
        a(f'<polyline points="{d}" fill="none" stroke="{stroke}" stroke-width="2" stroke-linejoin="round"{da}{m}/>')

    def group(x, y, w, h, title, color):
        a(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="none" stroke="{color}" stroke-width="1.6" '
          f'stroke-dasharray="7 5"/>')
        text(x + 16, y + 24, title, 14, 700, color=color)

    def box(x, y, w, h, title, lines, fill=None, line=None):
        a(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{fill or c["svc"]}" '
          f'stroke="{line or c["svcline"]}" stroke-width="1.6"/>')
        text(x + 16, y + 28, title, 16, 700)
        for i, s in enumerate(lines):
            text(x + 16, y + 52 + i * 21, s, 13, color=c["text"] if i == 0 else c["sub"])

    def label(x, y, s, color=None, anchor="middle"):
        text(x, y, s, 12, 600, color=color or c["sub"], anchor=anchor)

    # GitHub
    group(24, 40, 290, 450, "GitHub", c["sub"])
    box(44, 80, 250, 96, "저장소", ["작업 브랜치 → develop → main", "main 머지는 직접 함"])
    box(44, 214, 250, 150, "GitHub Actions", ["PR마다 테스트",
                                              "main 머지 → 이미지 빌드,",
                                              "ECR에 올리고 대기 버전 기록",
                                              "긴급 버튼 → 긴급 버전 기록",
                                              "(장중에는 적용 안 됨)"])
    path([(169, 176), (169, 214)])

    # AWS
    group(344, 40, 816, 450, "AWS", c["busline"])
    box(430, 84, 220, 76, "ECR", ["서비스별 이미지"], fill=c["ext"], line=c["extline"])
    box(430, 200, 220, 96, "Parameter Store", ["대기·긴급 버전", "비밀 설정(서버만 읽음)"], fill=c["ext"], line=c["extline"])
    box(430, 384, 220, 76, "S3", ["DB 백업"], fill=c["ext"], line=c["extline"])
    # Actions → ECR, Parameter Store
    path([(294, 240), (384, 240), (384, 122), (430, 122)], "b")
    label(392, 186, "이미지 올림", c["blue"], "start")
    path([(294, 268), (430, 268)], "b")
    label(392, 290, "버전 기록", c["blue"])

    # Lightsail
    group(710, 64, 430, 410, "Lightsail 서버 2대 (Docker Compose)", c["svcline"])
    box(728, 100, 394, 160, "서비스 서버", ["서비스 5개 + 배포 전 DB 준비(마이그레이션)",
                                         "배포 타이머: 매일 07:00 대기 버전 적용",
                                         "5분마다 긴급 버전 확인(대기 버전과 같을 때만)",
                                         "바뀐 것이 있으면 이미지를 받아 다시 띄움"])
    box(728, 330, 394, 126, "데이터 서버", ["Kafka · PostgreSQL",
                                         "백업 타이머: 매주 일요일 03:30",
                                         "덤프를 끝까지 받은 것만 S3에 올림"])
    # 서버가 받아 오기(pull): 화살표는 자료가 흐르는 방향
    path([(650, 122), (728, 122)], "p")
    label(680, 112, "받아 옴", c["purple"])
    path([(650, 232), (728, 232)], "p")
    label(680, 222, "읽기", c["purple"])
    path([(728, 422), (650, 422)], "p")
    label(680, 412, "올림", c["purple"])
    # 서버끼리 사설 주소
    path([(925, 260), (925, 330)], both=True)
    label(935, 300, "사설 주소로만 (Kafka·DB 접속)", anchor="start")

    # 바깥으로 나가는 연결만
    box(1180, 120, 100, 120, "외부", ["시세·뉴스", "Claude API", "메일 발송"], fill=c["ext"], line=c["extline"])
    path([(1122, 180), (1180, 180)])

    # 범례
    ly = 526
    text(24, ly, "들어오는 연결 없음: 서버가 먼저 받아 오고 내보내기만 합니다. 관리 접속은 Lightsail 콘솔의 브라우저 SSH만 씁니다.", 12.5, 600)
    a('</svg>')
    return "\n".join(out)


if __name__ == "__main__":
    for t in THEMES:
        with open(f"{sys.argv[1]}/deploy-{t}.svg", "w", encoding="utf-8") as f:
            f.write(svg(t))

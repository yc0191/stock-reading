# 설정 값 가져오기 (Parameter Store, 선택)

비밀값과 실행 설정은 AWS Systems Manager Parameter Store에 두고, `scripts/load_config.py`로 `.env`에 가져옵니다. 서비스는 계속 환경변수만 읽습니다.

## 그룹

| 그룹 (`--only` 값) | 키 대응의 원본 | 없을 때 |
|---|---|---|
| `notifier` (메일 설정) | `ENV_NAMES` | 필수입니다. 하나라도 없으면 아무것도 쓰지 않고 멈춥니다 |
| `collector` | `COLLECTOR_ENV_NAMES` | 선택입니다. 없는 키는 "참고:"로 알리고 건너뜁니다 |
| `briefing` (LLM 키) | `BRIEFING_ENV_NAMES` | 선택입니다. 키가 없으면 브리핑이 규칙 기반 리포트를 만듭니다 |

파라미터 경로는 같은 파일의 `PREFIX`, `COLLECTOR_PREFIX`, `BRIEFING_PREFIX`입니다. `--only`를 주지 않으면 세 그룹을 모두 가져옵니다. `--only collector`나 `--only briefing`을 주면 메일 설정은 확인하지 않습니다.

## 명령

```bash
python3 scripts/load_config.py --dry-run                # 파일을 쓰지 않고 반영할 키 이름만 출력
python3 scripts/load_config.py                          # 세 그룹 모두 .env에 반영 (관리하는 키만 바꾸고 나머지는 그대로)
python3 scripts/load_config.py --only collector         # 한 그룹만 (notifier, collector, briefing)
python3 scripts/load_config.py --only collector --check # .env가 Parameter Store와 같은지 비교
```

## `--check`

- 실서버를 부르기 전에 돌립니다. `.env`는 복사본이라 원본과 어긋날 수 있습니다.
- 이름마다 `같음`, `다름`, `없음`과 양쪽 값의 길이만 보여 줍니다. 파일은 쓰지 않습니다.
- 로더가 관리하지 않는 앱키·시크릿 이름이 `.env`에 남아 있으면 경고합니다.
- 어긋남이나 경고가 있으면 종료 코드 1을 돌려줍니다.

## 규칙

- 값은 어디에도 출력하지 않습니다.
- `NOTIFIER_SEND_ENABLED`는 가져오지 않습니다. 실수로 발송이 켜지지 않도록 사람이 `.env`에서만 바꿉니다.
- 자격증명은 `AWS_PROFILE` 환경변수나 `--profile` 옵션으로 지정합니다. 지역을 고르는 순서는 `--region` 도움말에 있습니다.
- 읽기 전용 IAM 자격증명이 필요합니다: [`infra/iam/README.md`](../../infra/iam/README.md)

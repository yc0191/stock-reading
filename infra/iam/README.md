# IAM: 설정 읽기 전용 사용자 (로컬용)

- `config-reader.policy.template.json`이 **최종(읽기 전용)** 정책입니다. `${ACCOUNT_ID}`, `${REGION}`은 적용할 때 채우며, **실제 계정 번호는 저장소에 넣지 않습니다.**
- 템플릿의 `Resource`(`parameter/stock-reading/dev/*`)는 `scripts/load_config.py`가 읽는 세 경로를 모두 덮습니다: `/stock-reading/dev/notifier/`, `/stock-reading/dev/collector/`, `/stock-reading/dev/briefing/`. 브리핑 경로(`anthropic-api-key` → `ANTHROPIC_API_KEY`)도 이 와일드카드에 포함되므로 템플릿을 바꾸지 않습니다.

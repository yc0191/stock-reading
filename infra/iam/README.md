# IAM: 설정 읽기 전용 사용자 (로컬용)

`stock-reading-config-reader-local`: Parameter Store의 `/stock-reading/dev/*` 경로를 **읽기만** 할 수 있는 IAM 사용자입니다(ADR-015). 콘솔 로그인 없음, 액세스 키는 로컬 AWS 자격증명 파일에만 둡니다.

- `config-reader.policy.template.json`이 **최종(읽기 전용)** 정책입니다. `${ACCOUNT_ID}`, `${REGION}`은 적용할 때 채우며, **실제 계정 번호는 저장소에 넣지 않습니다.** (콘솔 JSON 편집기에서는 계정 칸을 `*`로 두어도 경로·리전으로 범위가 좁혀집니다.)
- **최초 등록 때만** 아래 문장을 **임시로** 더해 값을 등록하고, 끝나면 **반드시 제거**해 최종(읽기 전용) 정책으로 되돌립니다.

```json
{
  "Sid": "TemporaryRegistrationRemoveAfterUse",
  "Effect": "Allow",
  "Action": ["ssm:PutParameter", "ssm:AddTagsToResource"],
  "Resource": "arn:aws:ssm:${REGION}:${ACCOUNT_ID}:parameter/stock-reading/dev/*"
}
```

- 운영(EC2)에서는 **사용자·키를 쓰지 않고**, 같은 읽기 정책을 **인스턴스 역할**에 붙입니다(경로만 `/stock-reading/prod/*`).

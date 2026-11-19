# 월 지출 알림. 알림만 보내고 자원을 멈추지 않는다(Budget actions 는 운영을 끊으므로 쓰지 않는다).
# 예산은 계정 전체에 하나로 잡히며 리전과 상관없다. 크레딧·환불을 뺀 금액(실제로 낼 돈)으로 센다(AWS 기본).
resource "aws_budgets_budget" "monthly" {
  name         = "${var.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  # 이달 쓰는 속도로 보아 월말에 한도를 넘을 것 같으면 미리 알린다. 묶음을 올린 달처럼 비용이 바뀐 뒤 가장 먼저 울린다.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}

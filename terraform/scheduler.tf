# 1. Regra de Agendamento (Cron Job)
# Roda todos os dias às 19:00 UTC
resource "aws_cloudwatch_event_rule" "daily_etl_trigger" {
  name                = "daily_b3_etl_trigger"
  description         = "Dispara a Extração da B3 todos os dias as 19:00 UTC"
  schedule_expression = "cron(0 19 * * ? *)"
  tags                = local.default_tags

  depends_on = [aws_iam_policy_attachment.github_actions_deploy_attachment]
}

# 2. O Agendador chama a Lambda de EXTRAÇÃO
resource "aws_cloudwatch_event_target" "trigger_lambda_target" {
  rule      = aws_cloudwatch_event_rule.daily_etl_trigger.name
  target_id = "TriggerExtractLambda"
  arn       = aws_lambda_function.extract_lambda.arn
}

# 3. Permissão: O EventBridge precisa de permissão para invocar a Lambda de Extração
resource "aws_lambda_permission" "allow_cloudwatch_extract" {
  statement_id  = "AllowExecutionFromCloudWatchExtract"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_etl_trigger.arn
}
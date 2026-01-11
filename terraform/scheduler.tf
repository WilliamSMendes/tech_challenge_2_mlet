# 1. Regra de Agendamento (Cron Job)
resource "aws_cloudwatch_event_rule" "daily_etl_trigger" {
  name        = "daily_b3_etl_trigger"
  description = "Dispara o ETL da B3 todos os dias as 09:00 UTC"
  schedule_expression = "cron(0 9 * * ? *)" 
  tags = local.default_tags
}

# 2. O Agendador chama a Lambda
resource "aws_cloudwatch_event_target" "trigger_lambda_target" {
  rule      = aws_cloudwatch_event_rule.daily_etl_trigger.name
  target_id = "TriggerLambdaGlue"
  arn       = aws_lambda_function.s3_trigger_glue.arn
}

# 3. Permissão: A Lambda precisa aceitar ser invocada pelo EventBridge
resource "aws_lambda_permission" "allow_cloudwatch_to_call_lambda" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_trigger_glue.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_etl_trigger.arn
}
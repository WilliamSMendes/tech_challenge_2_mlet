# 1. Empacotar o código Python da Lambda
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "../functions/trigger_glue.py"
  output_path = "../functions/trigger_glue.zip"
}

# 2. Criar a Role da Lambda (Permissões de Log e Glue)
resource "aws_iam_role" "lambda_exec_role" {
  name = "lambda_glue_trigger_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
  tags = local.default_tags
}

# Política para permitir iniciar Job Glue e Logar no CloudWatch
resource "aws_iam_policy" "lambda_policy" {
  name = "LambdaGlueTriggerPolicy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
  tags = local.default_tags
}

resource "aws_iam_role_policy_attachment" "lambda_attach" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# 3. Criar a Função Lambda
resource "aws_lambda_function" "s3_trigger_glue" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "s3_trigger_glue_transform"
  role             = aws_iam_role.lambda_exec_role.arn
  handler          = "trigger_glue.lambda_handler"
  runtime          = "python3.9"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 60

  tags = local.default_tags
}

# 4. Dar permissão ao S3 para invocar a Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_trigger_glue.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data_lake_bucket.arn
}

# 5. Configurar o Gatilho (Notification) no Bucket
# Importante: Isso cria a ligação Bucket -> Lambda
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_trigger_glue.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"    # Só dispara se cair na pasta raw
    filter_suffix       = ".parquet" # Opcional, mas boa prática
  }

  depends_on = [aws_lambda_permission.allow_s3]
}
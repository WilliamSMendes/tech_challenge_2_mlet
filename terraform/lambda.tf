# Zip da função de Extração
data "archive_file" "extract_lambda_zip" {
  type        = "zip"
  source_dir  = "../functions/package"
  output_path = "../functions/extract.zip"
}

# Zip da função de Gatilho
data "archive_file" "trigger_lambda_zip" {
  type        = "zip"
  source_file = "../functions/trigger_glue.py"
  output_path = "../functions/trigger_glue.zip"
}

# IAM Roles e Policies 
resource "aws_iam_role" "lambda_exec_role" {
  name = "lambda_b3_etl_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
  tags = local.default_tags
}

# Política unificada: Logs, Glue (StartJob) e S3 (Put/Get para salvar/ler arquivos)
resource "aws_iam_policy" "lambda_policy" {
  name = "LambdaETLPolicy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        # Dá permissão para ler/escrever no bucket do Data Lake
        Resource = [
          aws_s3_bucket.data_lake_bucket.arn,
          "${aws_s3_bucket.data_lake_bucket.arn}/*"
        ]
      }
    ]
  })
  tags = local.default_tags
}

resource "aws_iam_role_policy_attachment" "lambda_attach" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# Lambda de extração
resource "aws_lambda_function" "extract_lambda" {
  filename         = data.archive_file.extract_lambda_zip.output_path
  function_name    = "b3_extract_function"
  role             = aws_iam_role.lambda_exec_role.arn
  handler          = "extract.lambda_handler"
  runtime          = "python3.9"
  source_code_hash = data.archive_file.extract_lambda_zip.output_base64sha256
  timeout          = 300

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.data_lake_bucket.bucket
    }
  }

  # Layer para AWS SDK Pandas
  layers = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:12"]

  tags = local.default_tags
}

# Lambda de gatilho (Trigger Glue)
resource "aws_lambda_function" "s3_trigger_glue" {
  filename         = data.archive_file.trigger_lambda_zip.output_path
  function_name    = "s3_trigger_glue_transform"
  role             = aws_iam_role.lambda_exec_role.arn
  handler          = "trigger_glue.lambda_handler"
  runtime          = "python3.9"
  source_code_hash = data.archive_file.trigger_lambda_zip.output_base64sha256
  timeout          = 60

  tags = local.default_tags
}

# Permissão para o S3 invocar a lambda de trigger
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_trigger_glue.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data_lake_bucket.arn
}

# Notificação do Bucket: Quando cair algo em raw/, chame a trigger_glue
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_trigger_glue.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_permission.allow_s3]
}
# Glue Job - Transform
resource "aws_glue_job" "transform_job" {
  name              = "transform_job"
  description       = "Job responsible to transform raw data and save it on S3 Bucket"
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  execution_class   = "STANDARD"

  command {
    script_location = "s3://${aws_s3_bucket.source_code_bucket.bucket}/transform.py"
    name            = "glueetl"
    python_version  = "3"
  }

  default_arguments = {
    "--additional-python-modules" = "polars,yfinance"
    "--enable-continuous-logs"    = "true"
    "--enable-glue-datacatalog"   = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.default_tags
}

resource "aws_iam_role" "glue_job_role" {
  name = "GlueRole"
  tags = local.default_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "glue_policy" {
  name = "GluePolicy"
  tags = local.default_tags
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:*"]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action   = ["glue:*", "cloudwatch:*", "logs:*"]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
    ]
  })
}

resource "aws_iam_policy_attachment" "glue_attachment" {
  name       = "glue-attachment"
  roles      = [aws_iam_role.glue_job_role.name]
  policy_arn = aws_iam_policy.glue_policy.arn
}

resource "aws_s3_object" "transform_code" {
  depends_on = [aws_s3_bucket.source_code_bucket]

  bucket = aws_s3_bucket.source_code_bucket.id
  key    = "transform.py"
  source = "../src/transform.py"
  etag   = filemd5("../src/transform.py")
  tags   = local.default_tags
}

# Crawler para catalogar automaticamente os dados refinados no Glue Catalog
resource "aws_glue_crawler" "refined_crawler" {
  name          = "refined_crawler"
  role          = aws_iam_role.glue_job_role.arn
  database_name = "default"

  # Roda diariamente após o agendamento principal (19:00 BRT = 22:00 UTC)
  schedule = "cron(0 23 * * ? *)"

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake_bucket.bucket}/refined/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = local.default_tags
}
resource "aws_glue_job" "extract_job" {
  name              = "extract_job"
  description       = "Job responsible to extract raw data and save it on S3 Bucket"
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = "5.0"
  max_retries       = 1
  timeout           = 2880
  number_of_workers = 2
  worker_type       = "G.1X"
  execution_class   = "STANDARD"

  command {
    script_location = "s3://${aws_s3_bucket.source_code_bucket.bucket}/extract.py"
    name            = "glueetl"
    python_version  = "3"
  }

  default_arguments = {
    "--additional-python-modules" = "yfinance"
    "--enable-continuous-logs"    = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }  
}

# Glue Policy (Permissions)
resource "aws_iam_policy" "glue_policy" {
    name = "GluePolicy"
    policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:Get*",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Effect: "Allow",
        Action = [
            "cloudwatch:*"
        ],
        Resource: "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })  
}

# IAM role for Glue jobs
resource "aws_iam_role" "glue_job_role" {
  name = "GlueRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Upload Extract Script
resource "aws_s3_object" "extract_code" {
  depends_on = [aws_s3_bucket.source_code_bucket]
    
  bucket = aws_s3_bucket.source_code_bucket.id
  key    = "extract.py"
  source = "../src/extract.py"

  etag = filemd5("../src/extract.py")
}

# Attach policy on the Glue Role
resource "aws_iam_policy_attachment" "glue_attachment" {
  name       = "glue-attachment"  
  roles      = [aws_iam_role.glue_job_role.name]  
  policy_arn = aws_iam_policy.glue_policy.arn
}
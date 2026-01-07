resource "aws_athena_workgroup" "etl" {
  name = "etl_workgroup"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results_bucket.bucket}/query_results/"
    }
  }

  tags = local.default_tags
}

# Athena Policy (Permissions)
data "aws_iam_role" "athena_policy" {
  name = "AthenaPolicy"
}

resource "aws_iam_policy" "athena_policy" {
  name = "AthenaPolicy"
  tags = local.default_tags

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "athena:*",
        ]
        Effect   = "Allow"
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
      }
    ]
  })
}

# Attach policy on the GitHub Actions Role
resource "aws_iam_policy_attachment" "athena_attachment" {
  name       = "athena-attachment"
  roles      = [data.aws_iam_role.athena_policy.name]
  policy_arn = aws_iam_policy.athena_policy.arn
}
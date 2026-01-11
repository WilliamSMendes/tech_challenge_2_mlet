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
      }
    ]
  })
}

# Role used to manage Athena resources (GitHub Actions OIDC role)
data "aws_iam_role" "athena_execution_role" {
  name = "github-actions-web-identity"
}

# Attach policy on the GitHub Actions Role
resource "aws_iam_policy_attachment" "athena_attachment" {
  name       = "athena-attachment"
  roles      = [data.aws_iam_role.athena_execution_role.name]
  policy_arn = aws_iam_policy.athena_policy.arn
}

# GitHub Actions Role Permissions (Terraform deploy)
resource "aws_iam_policy" "github_actions_deploy_policy" {
  name = "GitHubActionsTerraformDeployPolicy"
  tags = local.default_tags

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "LambdaManagement"
        Effect = "Allow"
        Action = [
          "lambda:CreateFunction",
          "lambda:UpdateFunctionCode",
          "lambda:UpdateFunctionConfiguration",
          "lambda:DeleteFunction",
          "lambda:GetFunction",
          "lambda:GetFunctionConfiguration",
          "lambda:GetLayerVersion",
          "lambda:ListVersionsByFunction",
          "lambda:ListFunctions",
          "lambda:AddPermission",
          "lambda:RemovePermission",
          "lambda:TagResource",
          "lambda:UntagResource"
        ]
        Resource = "*"
      },
      {
        Sid      = "PassRoleForLambda"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = "*"
      },
      {
        Sid    = "EventBridgeManagement"
        Effect = "Allow"
        Action = [
          "events:PutRule",
          "events:DeleteRule",
          "events:DescribeRule",
          "events:PutTargets",
          "events:RemoveTargets",
          "events:ListTargetsByRule",
          "events:TagResource",
          "events:UntagResource"
        ]
        Resource = "*"
      },
      {
        Sid      = "TemporaryFullAccess"
        Effect   = "Allow"
        Action   = ["*"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "github_actions_deploy_attachment" {
  name       = "github-actions-deploy-attachment"
  roles      = [data.aws_iam_role.athena_execution_role.name]
  policy_arn = aws_iam_policy.github_actions_deploy_policy.arn
}
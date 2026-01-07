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
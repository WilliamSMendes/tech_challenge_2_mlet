# bbucket to hold glue jobs scripts in python
resource "aws_s3_bucket" "source_code_bucket" {
  bucket = "${local.account_id}-glue-source-code-bucket"
  tags = local.default_tags 
}

resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = "${local.account_id}-data-lake-bucket"
  tags = local.default_tags 
}

resource "aws_s3_bucket" "athena_results_bucket" {
  bucket = "${local.account_id}-athena-results-bucket"
  tags = local.default_tags 
}
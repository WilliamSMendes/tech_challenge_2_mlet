# bbucket to hold glue jobs scripts in python
resource "aws_s3_bucket" "source_code_bucket" {
  bucket = "tc-v2-source-code-bucket"

  tags = {    
    environment = "Dev"
    language = "python"
  }
}
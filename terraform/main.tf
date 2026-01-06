terraform {
  backend "s3" {
    bucket = "tc-v2-mlet-fiap-tfstates"
    key    = "infra/challenge2.tfstate"
    region = "us-east-1"
  }
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

locals {
  default_tags = {
    Project     = "TechChallengeV2"
    Environment = "Dev"
  }
  account_id = "818392673747"
}
# Terraform configuration
terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# AWS Provider configuration
provider "aws" {
  region = var.region
  
  default_tags {
    tags = {
      Project     = "Natgateway"
      Environment = "production"
      ManagedBy   = "Terraform"
    }
  }
}
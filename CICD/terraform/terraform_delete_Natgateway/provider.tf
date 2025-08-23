<<<<<<< HEAD
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
||||||| 1d37792
=======
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
>>>>>>> 27dc2eb88286d4d1e425f957b4627d62bfb0cce6

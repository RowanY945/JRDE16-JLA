# Variables for the Terraform configuration

variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-2"
}

variable "linkedin_lambda_name" {
  description = "Name of the LinkedIn Lambda function"
  type        = string
  default     = "jla-lambda-linkedin-id"
}

variable "indeed_lambda_name" {
  description = "Name of the Indeed Lambda function"
  type        = string
  default     = "jla-lambda-indeed-id"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for job data"
  type        = string
  default     = "jla-jobids-pool"
}

variable "schedule_expression" {
  description = "EventBridge schedule expression"
  type        = string
  default     = "rate(2 hours)"
}

variable "wait_between_lambdas" {
  description = "Wait time between Lambda executions (seconds)"
  type        = number
  default     = 300
}

variable "final_wait_time" {
  description = "Final wait time before completion (seconds)"
  type        = number
  default     = 600
}

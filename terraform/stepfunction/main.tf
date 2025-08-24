terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Lambda function for Indeed job processing
/*
resource "aws_lambda_function" "indeed_job_processor" {
  function_name = "lambda-indeed-job-id"
  role         = "arn:aws:iam::${var.aws_account_id}:role/Scraper_lambda_function"
  
  # Actual values from AWS
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  filename         = "placeholder.zip"
  source_code_hash = "placeholder"
  timeout          = 300
  layers           = ["arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:layer:layer-lambda-job-id-python-312:${var.lambda_layer_version}"]
  publish          = false
  
  lifecycle {
    ignore_changes = [
      filename,
      source_code_hash,
    ]
  }

  tags = {
    Name        = "Indeed Job Processor"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# Lambda function for LinkedIn job processing
resource "aws_lambda_function" "linkedin_job_processor" {
  function_name = "lambda-linkedin-job-id"
  role         = "arn:aws:iam::${var.aws_account_id}:role/Scraper_lambda_function"
  
  # Actual values from AWS
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  filename         = "placeholder.zip"
  source_code_hash = "placeholder"
  timeout          = 900
  layers           = ["arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:layer:layer-lambda-job-id-python-312:${var.lambda_layer_version}"]
  publish          = false
  
  lifecycle {
    ignore_changes = [
      filename,
      source_code_hash,
    ]
  }

  tags = {
    Name        = "LinkedIn Job Processor"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

*/

# Step Function for job processing workflow
resource "aws_sfn_state_machine" "job_processor_workflow" {
  name     = "step-function-job-id"
  role_arn = "arn:aws:iam::${var.aws_account_id}:role/Step-Functions-Role"
  
  # Actual definition from AWS
  definition = jsonencode({
    Comment = "Job processing pipeline with intervals to avoid LinkedIn blocking"
    StartAt = "ProcessFirstLambda"
    States = {
      ProcessFirstLambda = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = "arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:function:lambda-indeed-job-id"
          "Payload.$" = "$"
        }
        Retry = [
          {
            ErrorEquals = [
              "Lambda.ServiceException",
              "Lambda.AWSLambdaException",
              "Lambda.SdkClientException"
            ]
            IntervalSeconds = 2
            MaxAttempts = 6
            BackoffRate = 2
          }
        ]
        Next = "WaitBetweenLambdas"
      }
      WaitBetweenLambdas = {
        Type = "Wait"
        Seconds = 300
        Comment = "Wait 5 minutes between Lambda executions to avoid rate limiting"
        Next = "ProcessSecondLambda"
      }
      ProcessSecondLambda = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = "arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:function:lambda-linkedin-job-id"
          "Payload.$" = "$"
        }
        Retry = [
          {
            ErrorEquals = [
              "Lambda.ServiceException",
              "Lambda.AWSLambdaException",
              "Lambda.SdkClientException"
            ]
            IntervalSeconds = 2
            MaxAttempts = 6
            BackoffRate = 2
          }
        ]
        Next = "FinalWait"
      }
      FinalWait = {
        Type = "Wait"
        Seconds = 600
        Comment = "Wait 10 minutes before allowing next execution"
        Next = "Success"
      }
      Success = {
        Type = "Succeed"
        Comment = "Job processing completed successfully"
      }
    }
  })

  tags = {
    Name        = "Job Processor Workflow"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

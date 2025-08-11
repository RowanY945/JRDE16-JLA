# Terraform and provider configuration is in providers.tf

# Data sources to reference existing Lambda functions
data "aws_lambda_function" "linkedin_lambda" {
  function_name = "jla-lambda-linkedin-id"
}

data "aws_lambda_function" "indeed_lambda" {
  function_name = "jla-lambda-indeed-id"
}

# IAM Role for Step Function
resource "aws_iam_role" "step_function_role" {
  name = "StepFunctionJobProcessingRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for Step Function
resource "aws_iam_role_policy" "step_function_policy" {
  name = "StepFunctionLambdaPolicy"
  role = aws_iam_role.step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          data.aws_lambda_function.linkedin_lambda.arn,
          data.aws_lambda_function.indeed_lambda.arn
        ]
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

# Step Function State Machine
resource "aws_sfn_state_machine" "job_processing_pipeline" {
  name     = "JobProcessingPipeline"
  role_arn = aws_iam_role.step_function_role.arn

  definition = jsonencode({
    Comment = "Job processing pipeline with intervals to avoid LinkedIn blocking"
    StartAt = "ProcessFirstLambda"
    States = {
      ProcessFirstLambda = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = data.aws_lambda_function.linkedin_lambda.arn
          "Payload.$"  = "$"
        }
        Retry = [
          {
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"]
            IntervalSeconds = 2
            MaxAttempts     = 6
            BackoffRate     = 2
          }
        ]
        Next = "WaitBetweenLambdas"
      }
      WaitBetweenLambdas = {
        Type    = "Wait"
        Seconds = 300
        Comment = "Wait 5 minutes between Lambda executions to avoid rate limiting"
        Next    = "ProcessSecondLambda"
      }
      ProcessSecondLambda = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = data.aws_lambda_function.indeed_lambda.arn
          "Payload.$"  = "$"
        }
        Retry = [
          {
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"]
            IntervalSeconds = 2
            MaxAttempts     = 6
            BackoffRate     = 2
          }
        ]
        Next = "FinalWait"
      }
      FinalWait = {
        Type    = "Wait"
        Seconds = 600
        Comment = "Wait 10 minutes before allowing next execution"
        Next    = "Success"
      }
      Success = {
        Type    = "Succeed"
        Comment = "Job processing completed successfully"
      }
    }
  })
}

# IAM Role for EventBridge
resource "aws_iam_role" "eventbridge_role" {
  name = "EventBridgeStepFunctionRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for EventBridge
resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "EventBridgeStepFunctionPolicy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "states:StartExecution"
        Resource = aws_sfn_state_machine.job_processing_pipeline.arn
      }
    ]
  })
}

# EventBridge Rule for Scheduling
resource "aws_cloudwatch_event_rule" "job_processing_schedule" {
  name                = "JobProcessingSchedule"
  description         = "Trigger job processing every 2 hours"
  schedule_expression = "rate(2 hours)"
}

# EventBridge Target
resource "aws_cloudwatch_event_target" "step_function_target" {
  rule      = aws_cloudwatch_event_rule.job_processing_schedule.name
  target_id = "JobProcessingTarget"
  arn       = aws_sfn_state_machine.job_processing_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

# Outputs
output "state_machine_arn" {
  description = "The ARN of the Step Function State Machine"
  value       = aws_sfn_state_machine.job_processing_pipeline.arn
}

output "eventbridge_rule_name" {
  description = "The name of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.job_processing_schedule.name
}

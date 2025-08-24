output "indeed_lambda_arn" {
  description = "ARN of the Indeed job processor Lambda function"
  value       = aws_lambda_function.indeed_job_processor.arn
}

output "linkedin_lambda_arn" {
  description = "ARN of the LinkedIn job processor Lambda function"
  value       = aws_lambda_function.linkedin_job_processor.arn
}

output "step_function_arn" {
  description = "ARN of the job processor Step Function"
  value       = aws_sfn_state_machine.job_processor_workflow.arn
}

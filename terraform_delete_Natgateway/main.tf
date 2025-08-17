<<<<<<< HEAD
# Step 1: Add a "data" source to look up the existing IAM role.
# This tells Terraform to find the role named "Databricks_NAT_Lambda" in your AWS account.
data "aws_iam_role" "existing_role" {
  name = "Databricks_NAT_Lambda"
}

# Step 2: The resource block for creating a new role has been removed.

# Step 3: Update the Lambda function to use the ARN from the data source.
resource "aws_lambda_function" "demo" {
  function_name = var.function_name_Delete
  # This line now points to the ARN of the existing role we found above.
  role          = data.aws_iam_role.existing_role.arn
  
  runtime       = var.runtime      
  handler       = var.handler 
  filename      = var.package_path  
  source_code_hash = filebase64sha256("./package.zip")
  timeout       = var.timeout
  memory_size   = var.memory_size
  architectures = var.architectures           

  environment {
    variables = {
      APP_ENV = "dev"
    }
  }
}

output "lambda_name" {
  value = aws_lambda_function.demo.function_name
}
||||||| 1d37792
=======
# Step 1: Add a "data" source to look up the existing IAM role.
# This tells Terraform to find the role named "Databricks_NAT_Lambda" in your AWS account.
data "aws_iam_role" "existing_role" {
  name = "Databricks_NAT_Lambda"
}

# Step 2: The resource block for creating a new role has been removed.

# Step 3: Update the Lambda function to use the ARN from the data source.
resource "aws_lambda_function" "demo" {
  function_name = var.function_name_Delete
  # This line now points to the ARN of the existing role we found above.
  role          = data.aws_iam_role.existing_role.arn
  
  runtime       = var.runtime      
  handler       = var.handler 
  filename      = var.package_path  
  source_code_hash = filebase64sha256("./package.zip")
  timeout       = var.timeout
  memory_size   = var.memory_size
  architectures = var.architectures           

  environment {
    variables = {
      APP_ENV = "dev"
    }
  }
}

output "lambda_name" {
  value = aws_lambda_function.demo.function_name
}
>>>>>>> 27dc2eb88286d4d1e425f957b4627d62bfb0cce6

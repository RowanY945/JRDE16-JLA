variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-2"
}

variable "function_name_create" {
  description = "Databricks-Natgateway-elasticIP-Create"
  type        = string
  default="Databricks-Natgateway-elasticIP-Create-CICD"
}
variable "function_name_delete" {
  description = "Databricks-Natgateway-elasticIP-delete"
  type        = string
  default= "Databricks-Natgateway-elasticIP-delete-CICD"
}
variable "function_name_linkedin" {
  description = "jobsid_linkedin"
  type        = string
  default="linkedin_jobid"
}
variable "function_name_indeed" {
  description = "jobsid_indeed"
  type        = string
  default="indeed_jobid"
}




variable "package_path"{
    description="the location of lambda code"
    type=string
    default="./package.zip"
}

variable "handler" {
  description = "whch is to commnet"
  type        = string
  default="main.handler"
}

variable "runtime" {
  description = "Lambda env used"
  type        = string
  default="python3.11"
}

variable "memory_size" {
  description = "memory size for lambda"
  type        = number
  default     = 512
}

variable "timeout" {
  description = "Lambda timeout。"
  type        = number
  default     = 900
}

variable "architectures" {
  description = "arm64 save more resouce"
  type        = list(string)
  default     = ["arm64"]
}

variable "environment_variables" {
  description = "pass for env"
  type        = map(string)
  default     = {}
}

variable "environment" {
  description = "Nama environment"
  type        = string
  default     = "commo-dev"
}

variable "api_configs" {
  description = "API configurations untuk datasources (metal, currency, fred, news_api)"
  type = map(object({
    url = string
    key = string
  }))
  
} 
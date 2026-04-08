variable "environment" {
  description = "Nama environment"
  type        = string
  default     = "commo-dev"
}

variable "api_configs" {
  type = map(object({
    url = string
    key = string
  }))
  default = {
    "metal" = {
      url = "https://api.metals.dev/v1/latest"
      key = "SD7F5FYSNYFICG8TPCGB7248TPCGB"
    },
    "currency" = {
      url = "https://api.currencyfreaks.com/v2.0/rates/latest"
      key = "226611e4e9774a0983d5cdb9ef9a9163"
    },
    "fred" = {
      url = "https://api.stlouisfed.org/fred/series/observations"
      key = "b8a65bef53decee34581e0c6bf48a208"
    },
    "news_api" = {
      url = "https://newsapi.org/v2/everything"
      key = "af2733ecc2974829b4c7e5f51d883050"
    }
  }
}
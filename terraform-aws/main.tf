# 1. S3 Bucket untuk Data Lake (Raw Data)
resource "aws_s3_bucket" "data_lake" {
  bucket = "global-commodity-data-lake-${var.environment}" # Nama harus unik sedunia
  
  tags = {
    Name        = "Data Lake"
    Environment = var.environment
  }
}

# Membuat Parameter untuk URL (Tipe String biasa karena tidak rahasia)
resource "aws_ssm_parameter" "api_urls" {
  for_each = var.api_configs
  
  name  = "/global-commodity/datasource/${each.key}/url"
  type  = "String"
  value = each.value.url
}

# Membuat Parameter untuk API KEY (Tipe SecureString karena rahasia)
resource "aws_ssm_parameter" "api_keys" {
  for_each = var.api_configs
  
  name  = "/global-commodity/datasource/${each.key}/api_key"
  type  = "SecureString"
  value = each.value.key
}

# 3. AWS Glue Catalog Database (Metadata untuk Query di Athena)
resource "aws_glue_catalog_database" "commodity_db" {
  name = "ds_commodity_staged"
}


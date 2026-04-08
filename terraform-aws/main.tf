# 1. S3 Bucket untuk Data Lake (Raw Data)
resource "aws_s3_bucket" "data_lake" {
  bucket = "global-commodity-data-lake-${var.environment}" # Nama harus unik sedunia
  
  tags = {
    Name        = "Data Lake"
    Environment = var.environment
  }
}

# 2. SSM Parameter Store (Secret Manager versi "Miskin" / Murah & Efektif)
resource "aws_ssm_parameter" "db_password" {
  name        = "/${var.environment}/database/password"
  description = "Password untuk Database Production"
  type        = "SecureString"
  value       = "SangatRahasia123!" # Di industri, ini biasanya diisi lewat variable
}

# 3. AWS Glue Catalog Database (Metadata untuk Query di Athena)
resource "aws_glue_catalog_database" "commodity_db" {
  name = "ds_commodity_staged"
}
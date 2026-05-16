# 1. S3 Bucket untuk Data Lake (Raw Data)
resource "aws_s3_bucket" "data_lake" {
  bucket = "global-commodity-data-lake-${var.environment}" # Nama harus unik sedunia
  
  tags = {
    Name        = "Data Lake"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_public_access_block" "example" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
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

# ==========================================
# IAM User with S3 & Athena Full Access
# ==========================================

resource "aws_iam_policy" "commodity_full_access" {
  name        = "global-commodity-full-access-${var.environment}"
  description = "Full access to S3, Athena, and Glue for commodity project"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:*"
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "athena:*"
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "glue:*"
        Resource = "*"
      }
    ]
  })
}

# IAM User
resource "aws_iam_user" "commodity_superset_user" {
  name = "global-commodity-user-${var.environment}"
}

# Attach policy ke user
resource "aws_iam_user_policy_attachment" "commodity" {
  user       = aws_iam_user.commodity_superset_user.name
  policy_arn = aws_iam_policy.commodity_full_access.arn
}

# Access Key
resource "aws_iam_access_key" "commodity_user_key" {
  user = aws_iam_user.commodity_superset_user.name
}

# Output
output "iam_access_key_id" {
  value     = aws_iam_access_key.commodity_user_key.id
  sensitive = true
}

output "iam_secret_access_key" {
  value     = aws_iam_access_key.commodity_user_key.secret
  sensitive = true
}
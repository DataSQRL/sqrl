variable "snowflake_user_arn" {
  type        = string
  description = "ARN for the Snowflake user"
}

variable "iceberg_table_external_id" {
  type        = string
  description = "External ID for the Iceberg table"
}

provider "aws" {
  region = "us-west-2"
}

# Create an S3 bucket
resource "aws_s3_bucket" "my_bucket" {
  bucket = "my-s3-bucket-aws-test"
  acl    = "private"
}

# Create a Glue Catalog Database
resource "aws_glue_catalog_database" "my_glue_database" {
  name = "my_glue_database"
}

# IAM policy for Snowflake to access the S3 bucket
resource "aws_iam_policy" "snowflake_s3_access" {
  name        = "SnowflakeS3Access"
  description = "Allow Snowflake to access specific S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:DeleteObject",
          "s3:DeleteObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        Resource = [
          "${aws_s3_bucket.my_bucket.arn}/*", # Default bucket name
          "${aws_s3_bucket.my_bucket.arn}"
        ],
        Condition = {
          StringLike = {
            "s3:prefix": ["*"]
          }
        }
      }
    ]
  })
}

# Trust relationship policy for Snowflake role
resource "aws_iam_role" "snowflake_role" {
  name               = "SnowflakeRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = var.snowflake_user_arn
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId": var.iceberg_table_external_id
          }
        }
      }
    ]
  })
}

# IAM policy for accessing Glue resources
resource "aws_iam_policy" "glue_access" {
  name        = "GlueAccess"
  description = "Allow access to Glue resources"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}


# Attach the S3 access policy to the Snowflake role
resource "aws_iam_role_policy_attachment" "snowflake_s3_attachment" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.snowflake_s3_access.arn
}

# Trust relationship policy for Glue role
resource "aws_iam_role" "glue_role" {
  name               = "GlueRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Effect = "Allow"
      }
    ]
  })
}

# Attach the Glue access policy to the Glue role
resource "aws_iam_role_policy_attachment" "glue_policy_attachment" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_access.arn
}

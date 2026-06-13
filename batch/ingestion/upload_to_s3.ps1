# Upload extracted ADS-B trace files to AWS S3.
# Replace the bucket with your real bucket locally.
# Do not hardcode private buckets in public documentation.

$localFolder = "extracted/"
$s3Bucket = "s3://YOUR_BUCKET_NAME/traces/"

Write-Host "Uploading $localFolder to $s3Bucket"

aws s3 cp $localFolder $s3Bucket --recursive

Write-Host "Upload complete."

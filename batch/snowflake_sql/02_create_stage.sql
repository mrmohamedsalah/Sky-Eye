CREATE OR REPLACE STAGE traces_stage
    URL = 's3://YOUR_BUCKET_NAME/traces/'
    STORAGE_INTEGRATION = MY_S3_INTEGRATION
    FILE_FORMAT = my_json_format;

LIST @traces_stage;

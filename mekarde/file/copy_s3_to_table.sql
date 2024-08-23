COPY $table_name $columns
        FROM '$s3_uri'
        ACCESS_KEY_ID '$access_key'
        SECRET_ACCESS_KEY '$secret_key'
        CSV
        IGNOREHEADER 1;
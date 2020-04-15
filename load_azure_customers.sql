copy into "SNOWPIPE_DEMO"."PUBLIC"."CUSTOMERS"
  from 'azure://dataeu.blob.core.windows.net/snowflake/sales/customers'
  credentials=(azure_sas_token='?XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
  file_format = (type = csv field_delimiter = ',' skip_header = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"')

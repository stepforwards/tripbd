drop table if exists ods_company_${yyyymmdd};
create external table ods_company_${yyyymmdd}(
  company_id int
  ,company_address string
  ,company_attr string
  ,company_boss string
  ,company_name string
  ,company_phone string
)stored as parquet
location '/sqoop/btrip_pg/${yyyymmdd}/company';
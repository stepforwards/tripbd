drop table if exists ods_customer_${yyyymmdd};
create external table ods_customer_${yyyymmdd}(
    customer_id int,
    customer_name string,
    sex string,
    certificate_type string,
    certificate_no string,
    address string,
    phoneno string,
    note string,
    taobao_account string,
    hyunit string
)stored as parquet
location '/sqoop/btrip_pg/${yyyymmdd}/tb_customer';
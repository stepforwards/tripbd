drop table if exists ods_custsource_${yyyymmdd};
create external table ods_custsource_${yyyymmdd}(
    cust_source_id int,
      hotel_id int,
      source_name string,
      status int,
      note string
)stored as parquet
location '/sqoop/btrip_pg/${yyyymmdd}/tb_cust_source';
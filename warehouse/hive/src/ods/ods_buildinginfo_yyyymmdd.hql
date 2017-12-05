drop table if exists ods_buildinginfo_${yyyymmdd};
create external table ods_buildinginfo_${yyyymmdd}(
    buildinginfo_id int,
    siteno string,
    address string,
    hotel_id int
)stored as parquet
location '/sqoop/btrip_pg/${yyyymmdd}/tb_buildinginfo';
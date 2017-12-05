drop table if exists ods_layers_${yyyymmdd};
create external table ods_layers_${yyyymmdd}(
      layers_id int,
      layers_name string,
      buildinginfo_id int
)stored as parquet
location '/sqoop/btrip_pg/${yyyymmdd}/tb_layers';
drop table if exists ods_roomtype_${yyyymmdd};
create external table ods_roomtype_${yyyymmdd}(
  room_type_id int,
  roomtype_name string,
  company_id int,
  old_room_type_id int
)stored as parquet
location '/sqoop/btrip_pg/${yyyymmdd}/tb_roomtype';
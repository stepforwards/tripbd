drop table if exists ods_roominfo_${yyyymmdd};
create external table ods_roominfo_${yyyymmdd}(
    roominfo_id int,
    roomno string,
    alldayprice string,
    halfdayprice string,
    hourprice string,
    roomarea string,
    description string,
    note string,
    telephoneno string,
    bednumber int,
    gatecardid string,
    roomname string,
    layers_id int,
    room_type_id int,
    cleaning_flag int,
    valid_flag int
)stored as parquet
location '/sqoop/btrip_pg/${yyyymmdd}/tb_roominfo';
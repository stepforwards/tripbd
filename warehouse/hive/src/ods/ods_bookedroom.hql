create external table if not exists ods_bookedroom(
    bookedroom_id int,
    livetype string,
    room_money string,
    booktime string,
    bookvalid_flag int,
    paymoney string,
    disable_reason string,
    booking_id int,
    roominfo_id int,
    disable_staff_id int,
    disable_time string,
    checkin_room_id int
)
partitioned by (operate_date string)
stored as parquet;
alter table ods_bookedroom drop partition(operate_date='${yyyymmdd}');
alter table ods_bookedroom add partition(operate_date='${yyyymmdd}') location '/sqoop/btrip_pg/${yyyymmdd}/tb_bookedroom';
create external table if not exists ods_booking(
    booking_id int,
      customer_id int,
      staff_id int,
      paytype string,
      paymoney string,
      valid_flag int,
      memo string,
      disable_reason string,
      operate_time string,
      disable_staff_id int,
      disable_time string,
      hotel_id int,
      sourcetype int,
      acct_id int,
      full_balance string,
      book_balance string,
      checkin_balance string,
      sum_fee string,
      derate_fee string,
      final_charge string,
      price_class int,
      price_type int
)
partitioned by (operate_date string)
stored as parquet;
alter table ods_booking drop partition(operate_date='${yyyymmdd}');
alter table ods_booking add partition(operate_date='${yyyymmdd}') location '/sqoop/btrip_pg/${yyyymmdd}/tb_booking';
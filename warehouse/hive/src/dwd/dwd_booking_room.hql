create table if not exists dwd_booking_room(
    booking_id int
    ,customer_id int
    ,staff_id int
    ,paytype string
    ,booking_paymoney decimal
    ,valid_flag int
    ,memo string
    ,booking_disable_reason string
    ,operate_time string
    ,booking_disable_staff_id int
    ,booking_disable_time string
    ,hotel_id int
    ,sourcetype int
    ,acct_id int
    ,full_balance decimal
    ,book_balance decimal
    ,checkin_balance decimal
    ,sum_fee decimal
    ,derate_fee decimal
    ,final_charge decimal
    ,price_class int
    ,bookedroom_id int
    ,livetype string
    ,room_money decimal
    ,booktime string
    ,bookvalid_flag int
    ,room_paymoney decimal
    ,room_disable_reason string
    ,roominfo_id int
    ,room_disable_staff_id int
    ,room_disable_time string
    ,checkin_room_id int
    ,cust_source_id int
    ,source_name string
    ,status int
    ,customer_name string
    ,sex string
    ,certificate_type string
    ,certificate_no string
    ,customer_address string
    ,phoneno string
    ,note string
    ,taobao_account string
    ,hyunit string
)
partitioned by (operate_date string)
stored as orc;
alter table dwd_booking_room drop partition(operate_date='${yyyymmdd}');
alter table dwd_booking_room add partition(operate_date='${yyyymmdd}');

insert overwrite table dwd_booking_room partition(operate_date='${yyyymmdd}')
select a.booking_id
       ,a.customer_id
       ,a.staff_id
       ,a.paytype
       ,a.paymoney
       ,a.valid_flag
       ,a.memo
       ,a.disable_reason
       ,a.operate_time
       ,a.disable_staff_id
       ,a.disable_time
       ,a.hotel_id
       ,a.sourcetype
       ,a.acct_id
       ,a.full_balance
       ,a.book_balance
       ,a.checkin_balance
       ,a.sum_fee
       ,a.derate_fee
       ,a.final_charge
       ,a.price_class
       ,b.bookedroom_id
       ,b.livetype
       ,b.room_money
       ,b.booktime
       ,b.bookvalid_flag
       ,b.paymoney
       ,b.disable_reason
       ,b.roominfo_id
       ,b.disable_staff_id
       ,b.disable_time
       ,b.checkin_room_id
       ,c.cust_source_id
       ,c.source_name
       ,c.status
       ,d.customer_name
       ,d.sex
       ,d.certificate_type
       ,d.certificate_no
       ,d.address customer_address
       ,d.phoneno
       ,d.note
       ,d.taobao_account
       ,d.hyunit
from ods_booking a
inner join ods_bookedroom b
on a.booking_id=b.booking_id
left join ods_custsource_${yyyymmdd} c
on a.sourcetype=c.cust_source_id
left join ods_customer_${yyyymmdd} d
on a.customer_id=d.customer_id
-- where b.booktime>='${month_first_day}' and b.booktime<='${month_last_day}'
where a.operate_date='${yyyymmdd}' and b.operate_date='${yyyymmdd}'
;
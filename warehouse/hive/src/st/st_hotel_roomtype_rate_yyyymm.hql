drop table if exists st_hotel_roomtype_rate_${yyyymm};
create table st_hotel_roomtype_rate_${yyyymm} as
select a.date_month
        ,a.hotel_id
        ,a.room_type_id
        ,a.roomtype_name
        ,a.room_num
        ,b.book_num
        ,b.checkin_num
        ,b.checkin_num*1.00/(a.room_num*day(from_unixtime(unix_timestamp('${yyyymmdd}','yyyyMMdd')))) checkin_rate
from dw_hotel_roomtype_${yyyymm} a
left join dw_room_book_checkin_${yyyymm} b
on a.date_month=b.date_month
and a.hotel_id=b.hotel_id
and a.room_type_id=b.room_type_id
;
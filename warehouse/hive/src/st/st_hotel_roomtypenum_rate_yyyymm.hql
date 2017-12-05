drop table if exists st_hotel_roomtypenum_rate_${yyyymm};
create table st_hotel_roomtypenum_rate_${yyyymm} as
select
        date_month
        ,hotel_id
        ,count(room_type_id) room_type_num
        ,sum(room_num) room_num
        ,sum(book_num) book_num
        ,sum(checkin_num) checkin_num
        ,sum(checkin_num)*1.00/(sum(room_num)*day(from_unixtime(unix_timestamp('${yyyymmdd}','yyyyMMdd')))) checkin_rate
from st_hotel_roomtype_rate_${yyyymm}
group by date_month
        ,hotel_id
;
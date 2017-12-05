drop table if exists st_hotel_roompricerange_rate_${yyyymm};
create table st_hotel_roompricerange_rate_${yyyymm} as
select a.date_month
        ,a.hotel_id
        ,a.pricerange
        ,a.room_num
        ,b.book_num
        ,b.checkin_num
        ,b.checkin_num*1.00/(a.room_num*day(from_unixtime(unix_timestamp('${yyyymmdd}','yyyyMMdd')))) checkin_rate
from dw_hotel_roompricerange_${yyyymm} a
left join dw_room_pricerange_checkin_${yyyymm} b
on a.date_month=b.date_month
and a.hotel_id=b.hotel_id
and a.pricerange=b.pricerange
;
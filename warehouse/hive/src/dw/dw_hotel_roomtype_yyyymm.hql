drop table if exists dw_hotel_roomtype_${yyyymm};
create table dw_hotel_roomtype_${yyyymm} as
select ${yyyymm} date_month
        ,hotel_id
        ,room_type_id
        ,roomtype_name
        ,count(roominfo_id) room_num
from dwd_roominfo_${yyyymmdd} a
group by ${yyyymm}
         ,hotel_id
         ,room_type_id
         ,roomtype_name
;

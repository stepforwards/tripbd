drop table if exists dw_room_pricerange_checkin_${yyyymm};
create table dw_room_pricerange_checkin_${yyyymm} as
select ${yyyymm} date_month
        ,a.hotel_id
        ,case when (b.alldayprice < 100) then '小于100'
        		 when (b.alldayprice >= 100 and alldayprice < 200) then '大于等于100小于200'
        		 when (b.alldayprice >= 200 and alldayprice < 300) then '大于等于200小于300'
        		 when (b.alldayprice >= 300) then '大于等于300'
        	end pricerange
        ,count(a.bookedroom_id) book_num
        ,sum(case when a.checkin_room_id is not null then 1 else 0 end) checkin_num
from dwd_booking_room a
left join dwd_roominfo_${yyyymmdd} b
on a.roominfo_id=b.roominfo_id
where a.booktime>=${month_first_day} and a.booktime<=${month_last_day}
group by ${yyyymm}
         ,a.hotel_id
         ,case when (b.alldayprice < 100) then '小于100'
                  		 when (b.alldayprice >= 100 and alldayprice < 200) then '大于等于100小于200'
                  		 when (b.alldayprice >= 200 and alldayprice < 300) then '大于等于200小于300'
                  		 when (b.alldayprice >= 300) then '大于等于300'
                  	end
;
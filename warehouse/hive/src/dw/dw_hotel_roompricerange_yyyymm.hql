drop table if exists dw_hotel_roompricerange_${yyyymm};
create table dw_hotel_roompricerange_${yyyymm} as
select
	${yyyymm} date_month
	,hotel_id
	,case when (alldayprice < 100) then '小于100'
		 when (alldayprice >= 100 and alldayprice < 200) then '大于等于100小于200'
		 when (alldayprice >= 200 and alldayprice < 300) then '大于等于200小于300'
		 when (alldayprice >= 300) then '大于等于300'
	end pricerange
	,count(roominfo_id) room_num
from dwd_roominfo_${yyyymmdd}
group by  ${yyyymm}
		,hotel_id
		,case when (alldayprice < 100) then '小于100'
		 when (alldayprice >= 100 and alldayprice < 200) then '大于等于100小于200'
		 when (alldayprice >= 200 and alldayprice < 300) then '大于等于200小于300'
		 when (alldayprice >= 300) then '大于等于300'
	end
;
package com.zhiyou.bd17.streaming

/**
  * Created by Administrator on 2017/12/5.
  */
object TestRegex {
  def main(args: Array[String]): Unit = {
    val regex = "JudgeNote\\((.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)\\)".r
    val string = "JudgeNote(2,艺龙,73,用户姓名73,中级,18,10,title,5.0,5.0,4.0,4.0,1.0,5月入住,0,评论内容)"
    string match {
      case regex(hotelId,platfrom,userId,userName,userLevel,judgeTimes,converCity,title,scoreSum,scoreDtll,scoreDtl2
      ,scoreDtl3,scoreDtl4,liveDate,isPlatfromOrdered,content) => println(hotelId,content)
    }
  }
}

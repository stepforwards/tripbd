package com.zhiyou.bd17.streaming.consensus

import java.sql.DriverManager

import com.zhiyou.bd17.streaming.StreamingUtil._

/**
  * Created by Administrator on 2017/12/5.
  */
object JudgeNoteAnaly {
  // 从kafka获取judge_note中的数据
  def getJudgeNote() = {
    val topics = Array("judge_note")
    val judgeNote = getStreamingFromKafka(topics)
    judgeNote.map(x=>x.value())
  }

  def calcJudgeScore() = {
    val judgeNote = getJudgeNote()
    judgeNote.mapPartitions(x=>{
      val date = "20171205" //dstream中获取的流数据中应该有每一条评论的时间
      val regex = "JudgeNote\\((.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)\\)".r
      for (judge <- x) yield {
        judge match {
          case regex(hotelId,platfrom,userId,userName,userLevel,judgeTimes,converCity,title,scoreSum,scoreDtll,scoreDtl2
          ,scoreDtl3,scoreDtl4,liveDate,isPlatfromOrdered,content) => ((date,hotelId,platfrom),(scoreSum,scoreDtll,scoreDtl2,scoreDtl3,scoreDtl4,content))
        }
      }
    })
  }

  case class ScoreDt1(judgeNum:Int
                      ,goodNum:Int
                      ,mediaNum:Int
                      ,badNum:Int
                      ,sumScore:Double
                      ,safeScore:Double
                      ,nvScore:Double
                      ,cleanScore:Double
                      ,serverScore:Double)



  def accumulateScore() = {
    val judgeScore = calcJudgeScore()
    // 总评论数，好评评论数，中评评论数，差评评论数，综合总评分和，安全平分和，隔音平分和，卫生平分和，服务评分和
    // 平均综合平分，平均安全平分，平均隔音平分，平均卫生平分，平均服务平分
    val batMerge = judgeScore.mapValues(x=>{
      ScoreDt1(1
        , if (x._1.toDouble > 3) 1 else 0
        , if (x._1.toDouble == 3) 1 else 0
        , if (x._1.toDouble < 3) 1 else 0
        , x._1.toDouble
        , x._2.toDouble
        , x._3.toDouble
        , x._4.toDouble
        , x._5.toDouble
      )
    }).reduceByKey((d1,d2)=>{
      ScoreDt1(d1.judgeNum+d2.judgeNum
        ,d1.goodNum+d2.goodNum
        ,d1.mediaNum+d2.mediaNum
        ,d1.badNum+d2.badNum
        ,d1.sumScore+d2.sumScore
        ,d1.safeScore+d2.safeScore
        ,d1.nvScore+d2.nvScore
        ,d1.cleanScore+d2.cleanScore
        ,d1.serverScore+d2.serverScore
      )
    })
    batMerge.updateStateByKey((seq:Seq[ScoreDt1],os:Option[ScoreDt1])=>{
      os match {
        case Some(state) => if (seq.size > 0) {
          val scoreDt1 = seq(0)
          Some(ScoreDt1(
            state.judgeNum+scoreDt1.judgeNum
            ,state.goodNum+scoreDt1.goodNum
            ,state.mediaNum+scoreDt1.mediaNum
            ,state.badNum+scoreDt1.badNum
            ,state.sumScore+scoreDt1.sumScore
            ,state.safeScore+scoreDt1.safeScore
            ,state.nvScore+scoreDt1.nvScore
            ,state.cleanScore+scoreDt1.cleanScore
            ,state.serverScore+scoreDt1.serverScore
          ))
        }else {
          os
        }
        case _ => if (seq.size > 0) {
          Some(seq(0))
        } else {
          None
        }
      }
    }).mapValues(x=>{
      (x,x.sumScore*1.00/x.judgeNum,x.safeScore*1.00/x.judgeNum,x.nvScore*1.00/x.judgeNum,x.cleanScore*1.00/x.judgeNum,x.serverScore*1.00/x.judgeNum)
    })
  }

  // 把state里面的数据，实时更新
  // 把state里面的数据，实时更新
  def persistState() = {
    val states = accumulateScore()
    states.foreachRDD(rdd=>{
      rdd.foreachPartition(msg=>{
        Class.forName("com.mysql.jdbc.Driver")
        val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata14","root","root");
        val sql = "insert into accumulateScore values(?,?,?,?,?,?,?,?)"
        val st = connection.prepareStatement(sql)
        for (record <- msg) {
          st.setString(1,record._1._1)
          st.setString(2,record._1._2)
          st.setString(3,record._1._3)
          st.setDouble(4,record._2._2)
          st.setDouble(5,record._2._3)
          st.setDouble(6,record._2._4)
          st.setDouble(7,record._2._5)
          st.setDouble(8,record._2._6)
          st.addBatch()
        }
        st.executeBatch()
        connection.close()
      })
    })
  }

  def badReviewWarning() = {
    val judgeInfo = calcJudgeScore()
    judgeInfo.filter(_._2._1.toDouble < 3).foreachRDD(rdd=>{
      rdd.foreachPartition(msg=>{
        Class.forName("com.mysql.jdbc.Driver")
        val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata14","root","root");
        val sql = "insert into badReview values(?,?,?,?,?,?,?,?,?)"
        val st = connection.prepareStatement(sql)
        for (record <- msg) {
          st.setString(1,record._1._1)
          st.setString(2,record._1._2)
          st.setString(3,record._1._3)
          st.setInt(4,record._2._1.toDouble.toInt)
          st.setInt(5,record._2._2.toDouble.toInt)
          st.setInt(6,record._2._3.toDouble.toInt)
          st.setInt(7,record._2._4.toDouble.toInt)
          st.setInt(8,record._2._5.toDouble.toInt)
          st.setString(9,record._2._6)
          st.addBatch()
        }
        st.executeBatch()
        connection.close()
      })
    })
  }

  def main(args: Array[String]): Unit = {
//    getJudgeNote()
//    calcJudgeScore()
//    accumulateScore()
//    persistState()
    badReviewWarning()
    ssc.start()
    ssc.awaitTermination()
  }
}

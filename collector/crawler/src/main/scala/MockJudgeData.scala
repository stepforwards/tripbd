
import scala.util.Random

/**
  * Created by Administrator on 2017/12/5.
  */
// 模拟点评数据，并以avro的方式发送到flume
case class JudgeNote(hotleId: String
                     ,platform: String
                     ,userId: String
                     ,userName: String
                     ,userLevel: String
                     ,judgeTimes: Int
                     ,coverCity: Int
                     ,title: String
                     ,scoreSum: Double
                     ,scoreDt11: Double
                     ,scoreDt12: Double
                     ,scoreDt13: Double
                     ,scoreDt14: Double
                     ,liveDate: String
                     ,isPlateformOrdered: String
                     ,content: String)
object MockJudgeData {
  val random = new Random()
  val platforms = List("携程","艺龙","去哪儿","大众点评","美团")
  val userIds = (1 to 100).map(x=>(x,s"用户姓名$x"))
  val userLevels = List("新手","初级","中级","高级")

  def mkRandomJude() = {
    val hotelId = random.nextInt(20) + 1
    val platform = platforms(random.nextInt(5))
    val userId = userIds(random.nextInt(100))
    val userLevel = userLevels(random.nextInt(4))
    val judgeTimes = random.nextInt(20) //评论次数
    val coverCity = random.nextInt(15) // 遍布城市
    val title = "title"
    val scoreSum = random.nextInt(5) + 1
    val scoreDt11 = random.nextInt(5) + 1
    val scoreDt12 = random.nextInt(5) + 1
    val scoreDt13 = random.nextInt(5) + 1
    val scoreDt14 = random.nextInt(5) + 1
    val liveDate = s"${1+random.nextInt(12)}月入住"
    val isPlateformOrdered = random.nextInt(2) // 0 yes 1 no
    val content = "评论内容"
    JudgeNote(hotelId.toString,platform,userId._1.toString,userId._2,userLevel,judgeTimes,coverCity
      ,title,scoreSum,scoreDt11,scoreDt12,scoreDt13,scoreDt14,liveDate,isPlateformOrdered.toString,content)
  }

  def main(args: Array[String]): Unit = {
    val client = FlumeClient("master",8888)
    (1 to 100).foreach(i=>{
      val msg = mkRandomJude().toString
      client.sendString(msg)
      Thread.sleep(1000)
    })
    client.close()
  }

}

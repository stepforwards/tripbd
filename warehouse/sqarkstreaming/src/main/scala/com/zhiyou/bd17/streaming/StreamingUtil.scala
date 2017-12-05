package com.zhiyou.bd17.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}


/**
  * Created by Administrator on 2017/12/5.
  */
object StreamingUtil {
  val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
  val ssc = new StreamingContext(conf,Duration(5000))
  ssc.checkpoint("btripstreaming")

  def getStreamingFromKafka(topics:Array[String]) = {
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "master:9092,master:9093",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )

    KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent
      ,ConsumerStrategies.Subscribe[String,String](topics,kafkaParams))
  }
}

package com.zhiyou.bd17.sqoop

import org.apache.sqoop.client.SqoopClient

/**
  * Created by ThinkPad on 2017/12/1.
  */
object SQClient {
  val url = "http://centos1:12000/sqoop/"
  val client = new SqoopClient(url)
}

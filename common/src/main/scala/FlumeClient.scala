import java.nio.charset.Charset

import org.apache.flume.api.RpcClientFactory
import org.apache.flume.event.EventBuilder

/**
  * Created by Administrator on 2017/12/5.
  */
class FlumeClient(hostname: String,port: Int) {
  val client = RpcClientFactory.getDefaultInstance(hostname,port)

  def sendString(msg: String): Unit = {
    val event = EventBuilder.withBody(msg,Charset.forName("utf-8"))
    client.append(event)
    println(msg)
  }

  def close() = {
    client.close()
  }
}

object  FlumeClient{
  def apply(hostname: String,port: Int): FlumeClient = new FlumeClient(hostname,port)
}

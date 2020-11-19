package com.thalys.dc.sparkstreaming.source.selfdefined

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * descriptions:
  * 其实就是自定义接收器
  * 需要继承Receiver，并实现onStart、onStop方法来自定义数据源采集。
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 13:44
  */
object MySource{
  def apply(host: String, port: Int): MySource = new MySource(host, port)
}

class MySource(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  /*
  接收器启动的时候调用该方法. This function must initialize all resources (threads, buffers, etc.) necessary for receiving data.
  这个函数内部必须初始化一些读取数据必须的资源
  该方法不能阻塞, 所以 读取数据要在一个新的线程中进行.
   */
  override def onStart(): Unit = {

    // 启动一个新的线程来接收数据
    new Thread("Socket Receiver"){
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  // 此方法用来接收数据
  def receive()={
    val socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
    var line: String = null
    // 当 receiver没有关闭, 且reader读取到了数据则循环发送给spark
    while (!isStopped && (line = reader.readLine()) != null ){
      // 发送给spark
      store(line)
    }
    // 循环结束, 则关闭资源
    reader.close()
    socket.close()

    // 重启任务
    restart("Trying to connect again")
  }
  override def onStop(): Unit = {

  }

}

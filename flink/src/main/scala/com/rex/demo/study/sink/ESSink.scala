package com.rex.demo.study.sink


import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer

import org.apache.flink.streaming.api.scala._


/**
  * descriptions:
  * ES sink 示例
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 13:44
  */
object ESSink {
    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      val stream = env.fromCollection(List(
        "a",
        "b"
      ))

      val httpHosts = new java.util.ArrayList[HttpHost]
      httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

      val esSinkBuilder = new ElasticsearchSink.Builder[String](
        httpHosts,
        new ElasticsearchSinkFunction[String] {
          def createIndexRequest(element: String): IndexRequest = {
            val json = new java.util.HashMap[String, String]
            json.put("data", element)

            Requests.indexRequest()
              .index("my-index")
              .`type`("my-type")
              .source(json)
          }

          override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
            indexer.add(createIndexRequest(element))
          }
        }
      )

      // finally, build and add the sink to the job's pipeline
      stream.addSink(esSinkBuilder.build)

      env.execute
    }

  }

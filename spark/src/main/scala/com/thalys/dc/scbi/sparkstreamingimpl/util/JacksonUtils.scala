package com.thalys.dc.scbi.sparkstreamingimpl.util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 15:21
  */
object JacksonUtils {

    //配置ObjectMapper对象
    var objectMapper: ObjectMapper = new ObjectMapper()
      .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
      .registerModule(DefaultScalaModule)

    /**
      * 使用泛型方法，把json字符串转换为相应的JavaBean对象。
      *      转换为普通JavaBean：readValue(json,Student.class)
      *      转换为List:readValue(json,List.class).readValue(json,List.class)返回其实是List<Map>类型，第二个参数不能是List<Student>.class，所以不能直接转换。
      *      转换为特定类型的List，比如List<Student>，把readValue()的第二个参数传递为Student[].class.然后使用Arrays.asList();方法把得到的数组转换为特定类型的List。
      *      转换为Map：readValue(json,Map.class)
      *      我们使用泛型，得到的也是泛型
      *
      * @param content   要转换的JavaBean类型
      * @param valueType  原始json字符串数据
      * @return           JavaBean对象
      */
    def readValue[T](content: String , valueType: Class[T]) : T = {
        return objectMapper.readValue(content, valueType);
    }
}

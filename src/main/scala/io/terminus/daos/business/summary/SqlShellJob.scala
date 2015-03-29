package io.terminus.daos.business.summary

import io.terminus.daos.annotations.RequestMapping
import io.terminus.daos.core.SparkJob
import org.apache.spark.sql.hive.HiveContext

/**
 * @author wanggen on 2015-03-27.
 */
@RequestMapping(value = "/sql")
class SqlShellJob extends SparkJob {


   override def execute(): AnyRef = {
      val hiveContext = new HiveContext(sc)
      import hiveContext.sql
      val sqlStr = env("sql")
      val rows = sql(sqlStr)
      val builder = StringBuilder.newBuilder
      builder.append("[")
      sc.broadcast(builder)
      rows.toJSON.top(200).foreach{row => builder.append(row).append(",")}
      if(builder.length>1) builder.deleteCharAt(builder.length-1)
      builder.append("]")
      builder.toString()
   }


}

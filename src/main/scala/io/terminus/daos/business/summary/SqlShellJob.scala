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
//        hiveContext.setConf("spark.sql.codegen", "True")
        import hiveContext.sql
        val sqlStr = env("sql")
        val rows = sql(sqlStr)
        rows.toJSON.top(200).mkString("[", ",", "]")
    }


}

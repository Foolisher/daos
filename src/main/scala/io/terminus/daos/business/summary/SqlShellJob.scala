package io.terminus.daos.business.summary

import java.util.UUID

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import io.terminus.daos.annotations.RequestMapping
import io.terminus.daos.core.SparkJob
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/**
 * @author wanggen on 2015-03-27.
 */
@RequestMapping(value = "/sql")
class SqlShellJob extends SparkJob {


    import com.datastax.spark.connector.{SomeColumns, _}
    override def execute(): AnyRef = {

        val hiveContext = new HiveContext(sc)
        import hiveContext.sql

        hiveContext.udf.register("date", (d:Int)=>DateTime.now().plusDays(d).toString("YYYY-MM-dd"))
        hiveContext.udf.register("month", (m:Int)=>DateTime.now().plusMonths(m).toString("YYYY-MM"))

        val sqlStr = env("sql")
        val start = System.currentTimeMillis()

        env.getOrElse("async", "false") match {
            case "false" =>
                sql(sqlStr).toJSON.top(env.getOrElse("size", "200").toInt).mkString("[", ",", "]")

            case _ =>
                val date = DateTime.now().toString("YYYY-MM-dd")
                val ttl = env.getOrElse("ttl", 60 * 60 * 24 * 30).toString.toInt
                try {
                    CassandraConnector(conf).withSessionDo{
                        session=> session.execute(s"DELETE from groupon.spark_sql_job_results where job_id ='${env("job_id")}' and datetime ='$date'")
                    }

                    sql(sqlStr).toJSON.map { r => (env("job_id"), date, UUID.randomUUID().toString, r) }
                        .saveToCassandra("groupon", "spark_sql_job_results",
                                         columns = SomeColumns("job_id", "datetime", "rowid", "result"),
                                         writeConf = WriteConf(ttl = TTLOption.constant(ttl)))
                    saveStat(start, 1)
                } catch {
                    case e: Exception =>
                        sc.parallelize(Seq((env("job_id"), date, 1, compact(render("error" -> e.toString)))))
                            .saveToCassandra("groupon", "spark_sql_job_results",
                                             columns = SomeColumns("job_id", "datetime", "rowid", "result"),
                                             writeConf = WriteConf(ttl = TTLOption.constant(ttl)))
                        saveStat(start, -1)
                        throw e

                }
                "FINISH"
        }
    }

    def saveStat(start: Long, stat: Int) {
        val elapsed = (System.currentTimeMillis() - start) / 1000
        sc.parallelize(Seq((env("job_id"), elapsed, stat)))
            .saveToCassandra("groupon", "spark_sql_jobs",
                             SomeColumns("job_id", "last_time_cost", "last_exec_stat"))
    }

}

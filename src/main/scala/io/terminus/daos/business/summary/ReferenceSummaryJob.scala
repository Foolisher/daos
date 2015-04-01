package io.terminus.daos.business.summary

import com.datastax.spark.connector.{SomeColumns, _}
import io.terminus.daos.annotations.RequestMapping
import io.terminus.daos.core.SparkJob
import org.joda.time.DateTime

/**
 * @author wanggen on 2015-03-09.
 */
@RequestMapping(value = "/job/refsummary")
class ReferenceSummaryJob extends SparkJob {

    import org.apache.spark.SparkContext._

    override def execute(): AnyRef = {
        log.info(s"${getClass.getSimpleName} is starting...")
        val yesterday = DateTime.now().minusDays(1).toString("YYYY-MM-dd")
        val date = env.getOrElse("sumFor", yesterday)
        val Seq(_Y, _M, _D) = date.split("-").toSeq
        val warehouse = env.getOrElse("dataRoot", "/tmp")

        val regRDD = sc.textFile(warehouse + "/groupon_register_refs.csv")

        log.info(s"Ready for data groupon_register_refs of date:[$date]")

        case class Row(var yearMonth: String, var day:String, var `type`: Int, var total: Int, var registered: Int, var activated: Int)

        regRDD.map(_.split(","))
            .filter(_(13).split(" ")(0) == date)        // created_at=sumFor
            .map(arr => (arr(10).toInt, arr(9).toInt)) // (type, status)
            .groupByKey()
            .collect()
            .foreach { p =>
            val row = Row(_Y+_M, _D, p._1, 0, 0, 0)
            p._2.foreach { status =>
                row.total += 1
                if (status == 1) row.registered += 1
                if (status == 2) row.activated += 1
            }
            log.info(s"Row: $row")
            sc.parallelize(Seq(row))
                .saveToCassandra("groupon", "groupon_summary_references",
                                 SomeColumns("year_month", "day", "type", "total", "registered", "activated"))
        }
        log.info("Reference summar job done!")
        "ReferenceSummaryJob finished"

    }


}

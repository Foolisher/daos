package io.terminus.daos.business.summary

import com.datastax.spark.connector._
import io.terminus.daos.annotations.RequestMapping
import io.terminus.daos.core.SparkJob
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * <pre>
 * 用户引流统计
 * </pre>
 * @author wanggen on 2015-03-23.
 */
@RequestMapping(value = "/job/usersummary")
class UserSummaryJob extends SparkJob {


    case class User(var id: Long, var status: Int, var createdAt: String)

    case class UserProfile(var userId: Long, var channel: Int)

    override def execute(): AnyRef = {

        val yesterday = DateTime.now().minusDays(1).toString("YYYY-MM-dd")
        val sumFor = env.getOrElse("sumFor", yesterday)
        val date = env.getOrElse("sumFor", yesterday)
        val Seq(_Y, _M, _D) = date.split("-").toSeq
        val warehouse=env.getOrElse("dataRoot", "/tmp")

        val userRDD = sc.textFile(warehouse + "/ecp_users.csv")
            .map(_.replace("\\,", "").split(","))
            .map { r => (r(0).toLong, User(r(0).toLong, r(6).toInt, r(7).split(" ")(0)))}

        val userProfileRDD = sc.textFile(warehouse + "/ecp_user_profiles.csv")
            .map(_.replace("\\,", "").split(","))
            .map { r => (r(0).toLong, UserProfile(r(1).toLong, r(14).toInt))}

        val channelGroupedRDD: RDD[(Long, (User, UserProfile))] = userRDD.join(userProfileRDD).filter(_._2._1.status == 1)

        // ([channel], ([sumOfTotal], [sumOfSomeDay]))
        val rows = channelGroupedRDD.map { r =>
            (
                r._2._2.channel match { case 1 | 2 => 0; case 3 | 4 => 1 },
                (if (r._2._1.createdAt <= sumFor) 1 else 0, if (r._2._1.createdAt == sumFor) 1 else 0)
            )
        }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
            .collect().map(r => (_Y + _M, _D, r._1, r._2._1, r._2._2)).toSeq

        sc.parallelize(rows)
            .saveToCassandra("groupon", "groupon_summary_users",
                             SomeColumns("year_month", "day", "channel", "total", "net_increase"))

        "UserSummaryJob DONE"

    }
}

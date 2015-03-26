package io.terminus.daos.business.summary

import com.datastax.spark.connector.writer.{TimestampOption, WriteConf}
import com.datastax.spark.connector.{SomeColumns, _}
import io.terminus.daos.annotations.RequestMapping
import io.terminus.daos.core.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.joda.time.DateTime


/**
  * @author wanggen on 2015-03-10.
 */
@RequestMapping(value = "/job/dealsummary")
class DealSummaryJob extends SparkJob {

   override def init(): SparkContext = {
      new SparkContext(conf)
   }

   override def execute(): AnyRef = {
      log.info(s"${getClass.getSimpleName} is starting...")
      val yesterday=DateTime.now().minusDays(1).toString("YYYY-MM-dd")

      case class OrderItem(var orderId: Long, var id: Long, var fee: Long, var quantity: Int, var status: Int, var createdAt: String)
      val orderItemRDD = sc.textFile(s"${env.getOrElse("dataRoot", "/tmp")}/ecp_order_items.csv")
         .map { line =>
            val row = line.split(",")
            OrderItem(row(1).toLong, row(0).toLong, row(7).toLong, row(15).toInt, row(17).toInt, row(21).split(" ")(0))
         }
         .filter(_.createdAt == env.getOrElse("sumFor", yesterday))
         .cache()

      case class OrderPay(var orderId: Long, fee: Long, paidAt: String)
      val orderPayRDD = sc.textFile(s"${env.getOrElse("dataRoot", "/tmp")}/ecp_order_pays.csv")
         .map { line =>
               val row = line.replace("\\,", " ").split(",")
               val paidAt = if (row(14) == null) null else row(14).split(" ")(0)
               OrderPay(row(2).toLong, row(7).toLong, paidAt)
         }
         .filter(_.paidAt == env.getOrElse("sumFor", "/tmp"))
         .cache()


      // 总订单数量(包含支付及未支付)
      val grossOrder = orderItemRDD.map(p => (p.orderId, 1)).distinct().count()
      // 总订单中商品数量(包含支付及未支付)
      val grossItem = orderItemRDD.map(_.quantity).reduce(_ + _)
      // 总订单产生的金额(包含支付及未支付)
      val gmv = orderItemRDD.map(_.fee).reduce(_ + _)

      // 产生的支付总金额和
      val deal = orderPayRDD.map(_.fee).reduce(_ + _)
      // 产生的实际支付订单数量
      val dealOrder = orderPayRDD.count()
      // 产生的实际支付的商品销量和
      val dealItem = orderPayRDD.map(p => (p.orderId, 0)).join(orderItemRDD.map(p => (p.orderId, p.quantity)))
         .map(p => p._2._1 + p._2._2)
         .reduce(_ + _)
      // 实际支付订单平均金额
      val perOrder = deal / dealOrder

      log.info(
         s"""
                |  date      :${env.getOrElse("sumFor", "/tmp")}
                |  grossOrder:$grossOrder,
                |  grossItem :$grossItem,
                |  dealOrder :$dealOrder,
                |  dealItem  :$dealItem,
                |  gmv       :$gmv,
                |  deal      :$deal""".stripMargin
      )

      sc.parallelize(Seq((env.getOrElse("sumFor", "/tmp"), gmv,   grossOrder,    grossItem,    deal,   perOrder,    dealOrder,    dealItem)))
         .saveToCassandra("groupon", "groupon_summary_deals",
                          SomeColumns("sum_for",          "gmv", "gross_order", "gross_item", "deal", "per_order", "deal_order", "deal_item"))
      WriteConf(TimestampOption.)
      log.info("DealSummary job done!")

      "DealSummaryJob finished"
   }

}

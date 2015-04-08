package io.terminus.daos.business.summary

import com.datastax.spark.connector.{SomeColumns, _}
import io.terminus.daos.annotations.RequestMapping
import io.terminus.daos.core.SparkJob
import org.joda.time.DateTime


/**
 * @author wanggen on 2015-03-10.
 */
@RequestMapping(value = "/job/dealsummary")
class DealSummaryJob extends SparkJob {


    override def execute(): AnyRef = {
        log.info(s"${getClass.getSimpleName} is starting...")
        val yesterday = DateTime.now().minusDays(1).toString("YYYY-MM-dd")
        val date=env.getOrElse("sumFor", yesterday)
        val Seq(_Y, _M, _D) = date.split("-").toSeq
        val warehouse = env.getOrElse("dataRoot", "/tmp")

        case class OrderItem(var orderId: Long, var id: Long, var fee: Long, var quantity: Int, var status: Int, var createdAt: String)
        val orderItems = sc.textFile(s"$warehouse/ecp_order_items.csv")
            .map { line =>
            val row = line.split(",")
            OrderItem(row(1).toLong, row(0).toLong, row(7).toLong, row(15).toInt, row(17).toInt, row(21).split(" ")(0))
        }
            .filter(_.createdAt == date)
            .cache()


        // 总订单数量(包含支付及未支付)
        val grossOrder = orderItems.map(p => (p.orderId, 1)).countByKey().size
        // 总订单中商品数量(包含支付及未支付)
        val grossItem = orderItems.map(_.quantity).sum()
        // 总订单产生的金额(包含支付及未支付)
        val gmv = orderItems.map(_.fee).sum()


        val paidOrderItems = orderItems.filter(o => o.status != -6 && o.status != -7 && o.status != 0).cache()

        // 产生的支付总金额和
        val deal = paidOrderItems.map(_.fee).sum()
        // 产生的实际支付订单数量
        val dealOrder = paidOrderItems.map(_.orderId).distinct().count()
        // 产生的实际支付的商品销量和
        val dealItem = paidOrderItems.map(_.quantity).sum()
        // 实际支付订单平均金额
        val perOrder = deal / dealOrder

        log.info(
            s"""
                |  date      :$date
                |  grossOrder:$grossOrder,
                |  grossItem :$grossItem,
                |  dealOrder :$dealOrder,
                |  dealItem  :$dealItem,
                |  gmv       :$gmv,
                |  deal      :$deal""".stripMargin
        )

        sc.parallelize(Seq((_Y+_M, _D, gmv, grossOrder, grossItem, deal, perOrder, dealOrder, dealItem)))
            .saveToCassandra("groupon", "groupon_summary_deals",
                        SomeColumns("year_month", "day", "gmv", "gross_order", "gross_item", "deal", "per_order", "deal_order", "deal_item"))
        log.info("DealSummary job done!")
        "DealSummaryJob finished"
    }

}

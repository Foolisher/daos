package io.terminus.daos.core

import java.util.concurrent.{ThreadFactory, Executors, TimeUnit}

import akka.event.slf4j.Logger
import org.apache.spark.SparkConf

/**
 * <pre>
 * bootstrap for spark job
 * </pre>
 * @author wanggen on 2015-03-19.
 */
object Bootstrap {

    val log = Logger.apply("Bootstrap")

    val singleExecutor = Executors.newSingleThreadExecutor(new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
            new Thread(r, s"SparkJob executor")
        }
    })

    def main(args: Array[String]) {

        log.info("Launching spark job")

        new Thread(new Runnable {
            override def run(): Unit = {

                while (true) {
                    try {
                        log.info("tell alive"); TimeUnit.MINUTES.sleep(3)
                    } catch {
                        case e: Exception => log.error("Interrupted exception", e)
                    }
                }

            }
        }).start()


        val Seq(cassandraHost) = args.toSeq
        val conf = new SparkConf()
            .setAppName(getClass.getSimpleName)
            .set("spark.cassandra.connection.host", cassandraHost)
            .set("spark.cassandra.connection.rpc.port", "9160")
            .set("spark.driver.allowMultipleContexts", "true")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


        try {
            new DaosSocketServer bootstrap conf
        } catch {
            case e: Exception => log.error("Some thing wrong unknown", e)
        }

    }

}

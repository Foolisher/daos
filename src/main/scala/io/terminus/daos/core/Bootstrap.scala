package io.terminus.daos.core

import java.util.concurrent.TimeUnit

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

   def main(args: Array[String]) {

      log.info("Launching spark job")

      new Thread(new Runnable {
         override def run(): Unit = {

            while (true) {
               try {
                  log.info("tel alive"); TimeUnit.MINUTES.sleep(3)
               } catch{
                  case e: Exception => log.error("Interrupted exception", e)
               }
            }

         }
      }).start()


      val Seq(master, cassandraHost) = args.toSeq
      val conf = new SparkConf()
         .setMaster(master).setAppName(getClass.getSimpleName)
         .set("spark.cassandra.connection.host", cassandraHost)
         .set("spark.cassandra.connection.rpc.port", "9160")
         .set("spark.driver.allowMultipleContexts", "true")
         .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


      try{
         new DaosSocketServer bootstrap conf
      }catch {
         case e:Exception=>log.error("Some thing wrong unknown", e)
      }

   }

}

package io.terminus.daos.core

import akka.event.slf4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <pre>
 * abstract spark job
 * </pre>
 * @author wanggen on 2015-03-19.
 */

trait SparkJob extends Serializable {

    val log = Logger.apply(getClass.getSimpleName)

    @transient
    var conf: SparkConf    = _
    @transient
    var sc  : SparkContext = _

    var env: Map[String, String] = _

    /**
     * This is the entry point for a Spark Job Server to execute Spark jobs.
     * This function should create or reuse RDDs and return the result at the end, which the
     * Job Server will cache or display.
     * @return the job result
     */
    final def startJob(conf: SparkConf, env: Map[String, String]): AnyRef = {
        this.conf = conf
        this.env = env
        this.sc = new SparkContext(conf)
        try {
            execute()
        }
        finally sc.stop()
    }


    /**
     * The important logistic implementation of data analysis
     * @return Serializable result of the job result
     */
    def execute(): AnyRef


}

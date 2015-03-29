package io.terminus.daos.core

import java.io.{BufferedReader, InputStreamReader, OutputStream}
import java.net.ServerSocket
import java.nio.charset.Charset
import java.util.concurrent.{Executors, ThreadFactory}

import org.apache.spark.SparkConf
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * <pre>
 * Simple socket server for serving spark task RESTful api
 * </pre>
 * @author wanggen on 2015-03-19.
 */
class DaosSocketServer {

    val ok_header    = "HTTP/1.1 200 OK\n\n"
    val error_header = "HTTP/1.1 500 INTERNAL SERVER ERROR\n\n"

    val log = LoggerFactory.getLogger(getClass)

    var id        = 0
    val executors = Executors.newFixedThreadPool(4, new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
            id += 1
            new Thread(r, s"DaosSocketServer thread:[#$id]")
        }
    })


    def bootstrap(conf: SparkConf) = {

        val serverSocket = new ServerSocket(9005)
        log.info(s"Spark driver HOST:[${serverSocket.getInetAddress.getHostAddress}]")

        while (true) {

            val socket = serverSocket.accept()
            val t = System.currentTimeMillis()

            executors.execute(new Runnable {
                override def run(): Unit = {
                    log.info(s"New client:[${socket.getRemoteSocketAddress}] -- ${Thread.currentThread().getName}")

                    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
                    val out = socket.getOutputStream

                    try {

                        val headers = new ArrayBuffer[String]()
                        var line = in.readLine()
                        headers.append(line)

                        var result = ""
                        while (line != null) {
                            if (line == "" || line == "EOF") {
                                log.info(s"Headers => ${headers.toString()}")
                                headers(0).toUpperCase.split(" ")(0) match {
                                    case "GET" | "POST" =>
                                        val (context: String, env: Map[String, String]) = extractRequestContext(headers)
                                        if (urlMapping.contains(context))
                                            result = urlMapping(context).apply()
                                        else {
                                            val data = handlePostData(in, headers)
                                            log.info(s"Mixed post data: $data")
                                            result = ok_header + invokeSparkJob(conf, out, context, if (data.isEmpty) env else data ++ env)
                                        }
                                    case _ =>
                                        result = ok_header + note
                                }
                                log.debug("Http result:{}", result)
                                log.info(s"Close conn:[${socket.getRemoteSocketAddress}}] -- ${headers(0)}")
                                log.info(s"Request[${headers(0)}] cost [${System.currentTimeMillis() - t}] ms")
                                out.write(result.getBytes(Charset.forName("UTF-8")))
                                return
                            } else {
                                line = in.readLine()
                                headers.append(line)
                            }
                        }
                    } catch {
                        case e: Exception =>
                            out.write((error_header + e.toString).getBytes(Charset.forName("UTF-8")))
                            log.error("Job Request fail", e)
                    } finally {
                        in.close()
                        out.close()
                        socket.close()
                    }


                }
            })

        }

    }

    def invokeSparkJob(conf: SparkConf, out: OutputStream, context: String, env: Map[String, String]): String = {
        JobsHolder.mappingClasses(context)
            .newInstance
            .startJob(conf, env).toString
    }


    val note = """
                 | Bad command  Usage:
                 |   GET <host>:<port>/job/somejobname?p1=v1&p2=v2  [Enter]
                 | e.g. curl <host>:<port>/joblist   // to list job uri
               """.stripMargin


    def extractRequestContext(headers: ArrayBuffer[String]): (String, Map[String, String]) = {
        val uri = headers(0).split("\\s+")(1)
        val parts = uri.split("\\?")
        val context = parts(0)
        val map =
            if (parts.size == 1) Map.empty[String, String]
            else parts(1).split("&").map { q => val p = q.split("="); (p(0), p(1))}.toMap[String, String]
        log.info(s"env: $map")
        log.info(s"[GET] request uri:$uri")
        (context, map)
    }

    val urlMapping =
        Map(
            "/joblist" -> { () => ok_header + JobsHolder.mappingClasses.keySet.toString()},
            "/favicon.ico" -> { () => ok_header}
        )


    def handlePostData(in: BufferedReader, headers: ArrayBuffer[String]): Map[String, String] = {
        var len = 0
        for (h <- headers if h.startsWith("Content-Length"))
            len = h.split("\\s+")(1).toInt
        if (len == 0) return Map.empty
        val buf = new Array[Char](len)
        in.read(buf, 0, len)
        log.info(s"[POST] post content:${new String(buf)}")
        implicit val formats = DefaultFormats
        JsonMethods.parse(new String(buf)).extract[Map[String, String]]
    }


}

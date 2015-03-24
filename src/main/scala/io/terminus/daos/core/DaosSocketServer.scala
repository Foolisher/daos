package io.terminus.daos.core

import java.io.{BufferedReader, InputStreamReader}
import java.net.ServerSocket
import java.nio.charset.Charset
import java.util.concurrent.{Executors, ThreadFactory}

import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * <pre>
 * Simple socket server for serving spark task RESTful api
 * </pre>
 * @author wanggen on 2015-03-19.
 */
class DaosSocketServer {

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

                  while (line != null) {
                     if (line == "" || line == "EOF") {
                        log.info(s"Headers => ${headers.toString()}")
                        if (headers(0).toUpperCase.startsWith("GET ")) {

                           val (context: String, env: Map[String, String]) = extractRequestContext(headers)
                           if(context=="/joblist"){
                              out write JobsHolder.mappingClasses.keySet.toString().getBytes(Charset.forName("UTF-8"))
                           }

                           else
                           out write JobsHolder.mappingClasses(context)
                              .asInstanceOf[Class[SparkJob]]
                              .newInstance
                              .startJob(conf, env).toString.getBytes(Charset.forName("UTF-8"))

                        } else if (headers(0).toUpperCase.startsWith("POST ")) {

                           val (context: String, env: Map[String, String]) = extractRequestContext(headers)
                           out write JobsHolder.mappingClasses(context)
                              .asInstanceOf[Class[SparkJob]]
                              .newInstance()
                              .startJob(conf, env).toString.getBytes(Charset.forName("UTF-8"))

                        } else {
                           out.write(note.getBytes)
                        }

                        out.write("\nOK 200\n".getBytes)
                        log.info(s"Close conn:[${socket.getRemoteSocketAddress}}]")
                        log.info(s"Request cost [${System.currentTimeMillis() - t}] ms")
                        return
                     } else {
                        line = in.readLine()
                        headers.append(line)
                     }
                  }

               } catch {
                  case e: Exception => out.write((e + s"\n$note").getBytes); log.error("Job Request fail", e)
               } finally {
                  in.close()
                  out.close()
                  socket.close()
               }


            }
         })

      }


   }

   val note="""
              | *Bad command  Usage:
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


   def handlePost(in:BufferedReader, headers:ArrayBuffer[String]) {
      var len = 0
      for (h <- headers if h.startsWith("Content-Length"))
         len = h.split("\\s+")(1).toInt
      val buf = new Array[Char](len)
      in.read(buf, 0, len)
      log.info(s"[POST] post content:${new String(buf)}")
   }


}

package io.terminus.daos.core

import java.io.File

import io.terminus.daos.annotations.RequestMapping
import sclasner.{FileEntry, Scanner}

/**
 * <pre>
 * classpath class scanner
 * </pre>
 * @author wanggen on 2015-03-19.
 */
object JobsHolder {

    private def entryProcessor(acc: Seq[(String, String)], entry: FileEntry): Seq[(String, String)] = {
        if (entry.relPath.startsWith("io/terminus/daos")) {
            val fileName = entry.relPath.split(File.pathSeparator).last
            if (fileName.contains("$")) acc
            else acc :+(fileName.replace('/', '.').replace(".class", ""), "")
        } else {
            acc
        }
    }

    val mappingClasses = Scanner.foldLeft("io.terminus.daos", Seq.empty, entryProcessor)
        .map(Class forName _._1)
        .filter{ p => classOf[SparkJob].isAssignableFrom(p) && p.getAnnotation(classOf[RequestMapping]) != null }
        .map{ p => (p.getAnnotation(classOf[RequestMapping]).value(), p)}
        .toMap[String, Class[_]]

}

#!/bin/sh


rm io.terminus.daos
bin/spark-submit --conf spark.sql.shuffle.partitions=20 \
   --class io.terminus.daos.core.Bootstrap \
   --driver-class-path lib/mysql-connector-java-5.1.27.jar \
	--jars /usr/dev/workspace/terminus-daos/lib/daos-all.jar \
	/usr/dev/workspace/terminus-daos/target/daos-1.0-SNAPSHOT.jar \
	cassandra-host




rm io.terminus.daos
bin/spark-submit --conf spark.sql.shuffle.partitions=20 \
   --class io.terminus.daos.core.Bootstrap \
   --driver-class-path lib/mysql-connector-java-5.1.27.jar \
	/usr/dev/workspace/terminus-daos/lib/daos-all.jar
	cassandra-host


curl '10.0.0.8:9005/job/dealsummary?sumFor=2015-03-10&dataRoot=/tmp'
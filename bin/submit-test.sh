#!/bin/sh


rm io.terminus.daos
bin/spark-submit \
   --class io.terminus.daos.core.Bootstrap \
	--jars /usr/dev/workspace/terminus-daos/lib/daos-all.jar \
	/usr/dev/workspace/terminus-daos/target/daos-1.0-SNAPSHOT.jar \
	 "local[2]" wg-linux




rm io.terminus.daos
bin/spark-submit \
   --class io.terminus.daos.core.Bootstrap \
        /usr/dev/workspace/terminus-daos/lib/daos-all.jar \
		 "local[2]" wg-linux


curl '10.0.0.8:9005/job/dealsummary?sumFor=2015-03-10&dataRoot=/tmp'
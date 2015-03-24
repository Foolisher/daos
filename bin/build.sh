#!/bin/sh

BASE_DIR=`pwd`

DESC_ID=''

if [ $# = 0 ]; then
	echo -e "\033[31;1m Package project for local test \033[m"
	mvn clean package -Dmaven.test.skip=true
fi

while [ $# -gt 0 ]; do
	case $1 in
		-a)#assembly with development environment
			echo -e "\033[31;1m Prepare libs for project \033[m"
			DESC_ID=src/main/assembly/assembly.xml;
			mvn clean assembly:assembly -Ddescriptor=${DESC_ID} -Dmaven.test.skip=true
			shift
		;;
		-d)#deploy -- package all in one jar
			echo -e "\033[31;1m Package all libs for deploy \033[m"
			DESC_ID=jar-with-dependencies
			mvn clean assembly:assembly -Ddescriptor=${DESC_ID} -Dmaven.test.skip=true
			scp ${BASE_DIR}/lib/daos-all.jar admin@10.0.0.8:/usr/local/spark/workspace/daos/
			shift
		;;
		-p)#package -- only compile and package our project
			echo -e "\033[31;1m Package project for local test \033[m"
			mvn clean package -Dmaven.test.skip=true
			shift
		;;
		*)
			help=`cat <<EOF
	Usage: bin/build.sh <options>
	Options:
	  -a : [assembly] the dependencies avoid expensive whole project build
	  -d : [deploy] and build project
	  -p : [package] current project
EOF`
			echo -e "\033[1m${help}\033[m";
			shift
		;;
	esac
done



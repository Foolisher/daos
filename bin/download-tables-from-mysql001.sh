#!/bin/sh

TABLES=$1
DATE=`date +'%Y%m%d%H%M%S'`

for TABLE in `cat ${TABLES}`; do
	FILE=/tmp/${TABLE}-${DATE}.csv
	echo -e "\033[31;1m Importing table[${TABLE}] to /tmp/${TABLE}.csv\033[m"
	ssh admin@mysql001 \
	  mysql -h 192.168.1.15 -u root -panywhere -D  groupon <<EOF
	  select * from ${TABLE}
	   into outfile '$FILE'
	   fields terminated by ',' escaped by '\\\'
	   optionally enclosed by ''
	   lines terminated by '\n'
EOF

ssh admin@mysql001 scp admin@mysql002:${FILE} /tmp/${TABLE}.csv

scp admin@mysql001:/tmp/${TABLE}.csv /tmp/${TABLE}.csv
ssh admin@mysql001 '[ -f /tmp/${TABLE}.csv ] && sudo rm /tmp/${TABLE}.csv'
#ssh admin@mysql001 'ssh admin@mysql002 "[ -f ${FILE} ] && rm ${FILE}" '

echo -e "\033[32;1m Table[${TABLE}] downloaded to /tmp/${TABLE}.csv\033[m"

done


#!/bin/sh
APPID=`basename $0 .sh`
STAMP=`date +%Y%m%d%H%M%S`
SYSID=batch-slave
KEYWORDS="bootstrap.appid=$SYSID "

locateClasspath()
{
	classpath="."
	for i in `ls *.jar 2>/dev/null; ls lib/*.jar 2>/dev/null;`
	do
		classpath="$classpath:$i"
	done
	echo $classpath
}
checkService()
{
	pids=`ps -u "${LOGNAME}" -f | grep "$KEYWORDS" | grep -v grep | awk '{ printf("%s ", $2) }'`
	if [ -n "$pids" ]
	then
		echo "[$SYSID] the application is running, PID $pids"
		exit 1
	fi
}
initEnv()
{
	if [ ! -d "logs/jvm" ]
	then
		\mkdir -p logs/jvm
	fi
	if [ ! -d "tmp" ]
	then
		\mkdir -p tmp
	fi
}

####
#### custom jdk 
####
#JAVA_HOME="/usr/local/jdk1.7.0_79"
#export JAVA_HOME
PATH=%JAVA_HOME%/bin:$PATH
export PATH

echo ============================
echo [$SYSID] Loading Jars ...
echo ============================
CLASSPATH=`locateClasspath`
export CLASSPATH
if [ -z "${JAVA_OPTS}" ]
then
	JAVA_OPTS="-server -Xms256M -Xmx1096M "
	export JAVA_OPTS
fi
JAVA_GCOPTS="-Xloggc:logs/jvm/gc_${SYSID}_${STAMP}.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=68"
JAVA_DUMPOPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=logs/jvm"
JAVA_PROPOPTS="-Djava.io.tmpdir=`pwd`/tmp  -Dspring.profiles.active=slave "
export JAVA_GCOPTS
export JAVA_DUMPOPTS
export JAVA_PROPOPTS


echo ============================
echo [$SYSID] Check Service
echo ============================
checkService
initEnv

echo ============================
echo [$SYSID] Start Service
echo ============================
(
echo ${CLASSPATH}
echo ${JAVA_OPTS}
echo ${JAVA_GCOPTS}
echo ${JAVA_DUMPOPTS}
echo ${JAVA_PROPOPTS}
java ${JAVA_OPTS} ${JAVA_GCOPTS} ${JAVA_DUMPOPTS} ${JAVA_PROPOPTS} com.iceps.spring.launch.ApplicationRmq 2>&1
) | tee logs/${SYSID}_${STAMP}.log &

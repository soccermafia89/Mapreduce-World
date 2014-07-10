bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
HOME=$(dirname -- $bin)

echo "HOME: $HOME"

for JARFILE in `ls ${HOME}/target/lib/*`
do
        HADOOP_CLASSPATH=$JARFILE:${HADOOP_CLASSPATH}
done

MAIN_JAR=`ls ${HOME}/target/mapreduce-world*.jar`
HADOOP_CLASSPATH=$MAIN_JAR:${HADOOP_CLASSPATH}

export HADOOP_CLASSPATH=/etc/hadoop/conf:$HADOOP_CLASSPATH

LIB_JARS=`echo ${HADOOP_CLASSPATH} | sed -e "s/:/,/g"`

export HADOOP_CLIENT_OPTS="-Xmx2g $HADOOP_CLIENT_OPTS"
hadoop jar ${MAIN_JAR} ethier.alex.world.mapreduce.addon.TestProcessor -libjars ${LIB_JARS}

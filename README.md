# Hadoop

##start following systems:

java -classpath <path to protobuf jar> -Djava.rmi.server.hostname=<this sys ip> -Djava.security.policy=server.policy HDFSClient <NameNodeIP>
java -classpath <path to protobuf jar> -Djava.rmi.server.hostname=<this sys ip> -Djava.security.policy=server.policy NameNode
java -classpath <path to protobuf jar> -Djava.rmi.server.hostname=<this sys ip> -Djava.security.policy=server.policy DataNode

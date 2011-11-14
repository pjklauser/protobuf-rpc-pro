echo off

set CLASSPATH=..\lib\commons-logging-1.1.1.jar;..\lib\log4j-1.2.16.jar;..\lib\netty-3.2.6.Final.jar;..\lib\protobuf-java-2.4.1.jar;..\lib\protobuf-rpc-pro-demo-1.1.1.jar;..\lib\protobuf-rpc-pro-duplex-1.1.1.jar

set EXAMPLE_CLASS=com.googlecode.protobuf.pro.duplex.example.%1

set EXAMPLE_ARGS=-Dlog4j.configuration="file:..\lib\log4j.properties"

java -classpath %CLASSPATH% %EXAMPLE_ARGS% %EXAMPLE_CLASS% %2 %3 %4 %5 %6 %7 %8 %9

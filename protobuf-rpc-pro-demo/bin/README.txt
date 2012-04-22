--------------------------------------------------------------------
ONE-WAY client->server
--------------------------------------------------------------------

RunExample.cmd|.sh PingServer <serverHostname> <serverPort>
  runs a RPC server on port 9001
example
$ ./RunExample.cmd PingServer localhost 9001


RunExample.cmd|.sh PingClient <serverHostname> <serverPort> <clientHostname> <clientPort> <numCalls> <processingTimeMs> <payloadBytes> <compress Y/N>
  runs an RPC client which does <numCalls> blocking calls to the RPC server, where the server takes 
  <processingTimeMs> milliseconds sleep for each call and the payload of each call is <payloadBytes> bytes long.
example
$ ./RunExample.cmd PingClient localhost 9001 localhost 9002 100 0 100 N


--------------------------------------------------------------------
DUPLEX client<->server
--------------------------------------------------------------------

RunExample.cmd|.sh DuplexPingPongServer <serverHostname> <serverPort>
  runs a RPC server on port 9001 which for each ping call serviced, it calls pong
  on the calling client before responding
example
$ ./RunExample.cmd DuplexPingPongServer localhost 9001


RunExample.cmd|.sh DuplexPingPongClient <serverHostname> <serverPort> <clientHostname> <clientPort> <numCalls> <processingTimeMs> <payloadBytes> <compress Y/N>
  runs an RPC client which does <numCalls> blocking calls to the RPC server, where the server takes 
  <processingTimeMs> milliseconds sleep for each call and the payload of each call is <payloadBytes> bytes long.
example
$ ./RunExample.cmd DuplexPingPongClient localhost 9001 localhost 9002 100 0 100 100 N


--------------------------------------------------------------------
SSL client->server
--------------------------------------------------------------------

RunExample.cmd|.sh SecureServer <serverHostname> <serverPort>
  runs a secure RPC server on port 9001 
  at runtime the server needs the server.keystore and the truststore files in /lib
example
$ ./RunExample.cmd SecureServer localhost 9001


RunExample.cmd|.sh SecureClient <serverHostname> <serverPort> <clientHostname> <clientPort> <compress Y/N>
  runs a secure RPC client which calls pings the server a few times. 
  at runtime, the client needs the client.keystore and the truststore files in /lib
example
$ ./RunExample.cmd SecureClient localhost 9001 localhost 9002 N


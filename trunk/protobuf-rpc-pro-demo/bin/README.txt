PingPongServer
usage: <serverHostname> <serverPort>
  runs a RPC server on port 9001

$ ./RunExample.cmd PingPongServer localhost 9001



PingClient
usage: <serverHostname> <serverPort> <clientHostname> <clientPort> <numCalls> <processingTimeMs> <upSizeBytes> <downSizeBytes>
  runs an RPC client which does blocking calls to the RPC server.

$ ./RunExample.cmd PingClient localhost 9001 localhost 9002 100 0 100 100

sends 100x 100 bytes up to the server running on port 9001.



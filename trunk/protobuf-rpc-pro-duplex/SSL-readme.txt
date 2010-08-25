http://www.tc.umn.edu/~brams006/selfsign.html



HOWTO: create a your own self signed root CA


1) create your RSA Private Key. 
This key is a 2048 bit RSA key which is encrypted using Triple-DES and stored in a 
PEM format so that it is readable as ASCII text. 

$ openssl genrsa -des3 -out certificateauthority.key 2048

providing a passphrase ( changeme )



$ openssl req -new -x509 -days 1000 -key certificateauthority.key -out certificateauthority.crt
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) [AU]:CH
State or Province Name (full name) [Some-State]:Zug
Locality Name (eg, city) []:Zug
Organization Name (eg, company) [Internet Widgits Pty Ltd]:IT
Organizational Unit Name (eg, section) []:IT
Common Name (eg, YOUR name) []:ca
Email Address []:ca@trustworthy-inc.com


Remove passhprase from private key

$ cp certificateauthority.key certificateauthority.key.original
$ openssl rsa -in certificateauthority.key.original -out certificateauthority.key

[otherwise apache prompts for it on startup]
















CREATE a server cert

$ openssl genrsa -des3 -out server.key 2048
Generating RSA private key, 2048 bit long modulus
..............................................++++++
.....++++++
e is 65537 (0x10001)
Enter pass phrase for server.key:
Verifying - Enter pass phrase for server.key:


$ openssl req -new -key server.key -out server.csr 
Enter pass phrase for server.key:
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) [AU]:CH
State or Province Name (full name) [Some-State]:Zug
Locality Name (eg, city) []:Zug
Organization Name (eg, company) [Internet Widgits Pty Ltd]:my organization
Organizational Unit Name (eg, section) []:IT
Common Name (eg, YOUR name) []:server
Email Address []:server@myorganization.com

Please enter the following 'extra' attributes
to be sent with your certificate request
A challenge password []:
An optional company name []:




3) Remove passhprase from private key

$ cp server.key server.key.original
$ openssl rsa -in server.key.original -out server.key


SIGN with our CA cert.

$ openssl x509 -req -days 365 -in server.csr -CAkey certificateauthority.key -CA certificateauthority.crt -set_serial 01 -out server.crt
Signature ok
subject=/C=CH/ST=Zug/L=Zug/O=my organization/OU=IT/CN=server/emailAddress=server@myorganization.com
Getting CA Private Key


TRANSFORM private key to DER for for inporting into a JKS keystore

$ openssl pkcs8 -topk8 -nocrypt -in server.key -out server.key.der -outform der


TRANSFORM public key to DER format for inporting into the JKS keystore

$ openssl x509 -in server.crt -out server.crt.der -outform der

ADD both the public and private keys into the server.keystore

$ ./RunKeyTool.cmd server.key.der server.crt.der server.keystore changeme server-key

$ "$JAVA_HOME/bin/keytool" -list -keystore server.keystore
Enter keystore password:  pwd

Keystore type: JKS
Keystore provider: SUN

Your keystore contains 1 entry

server-key, Aug 23, 2010, PrivateKeyEntry, 
Certificate fingerprint (MD5): 3C:E9:38:06:23:DC:3D:0B:EA:F1:B3:4C:EC:BC:6D:5F





Generate a client key

$ openssl genrsa -des3 -out client.key 2048

$ openssl req -new -key client.key -out client.csr
Enter pass phrase for client.key:
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) [AU]:CH
State or Province Name (full name) [Some-State]:Zug
Locality Name (eg, city) []:Zug
Organization Name (eg, company) [Internet Widgits Pty Ltd]:my organization
Organizational Unit Name (eg, section) []:IT
Common Name (eg, YOUR name) []:client
Email Address []:client@myorganization.com

Please enter the following 'extra' attributes
to be sent with your certificate request
A challenge password []:
An optional company name []:


3) Remove passhprase from private key

$ cp client.key client.key.original
$ openssl rsa -in client.key.original -out client.key



SIGN client certificate with our server cert.

$ openssl x509 -req -days 365 -in client.csr -CAkey server.key -CA server.crt -set_serial 01 -out client.crt

Signature ok
subject=/C=CH/ST=Zug/L=Zug/O=my organization/OU=IT/CN=client/emailAddress=client@myorganization.com
Getting CA Private Key

convert client key to DER form

$ openssl pkcs8 -topk8 -nocrypt -in client.key -out client.key.der -outform der

convert client public cert to DER form

$ openssl x509 -in client.crt -out client.crt.der -outform der

join private and public client keys into the JKS client keystore

$ ./RunKeyTool.cmd client.key.der client.crt.der client.keystore changeme client-key

d:\Development\Workspace\protobuf-rpc-pro\protobuf-rpc-pro-duplex\ssl>java -classpath ..\target\protobuf-rpc-pro-duplex-1.0.0.jar com.googlecode.protobuf.pro.duplex.util.KeyStoreImportUtil client.key.der client.crt.der client.keystore pwd client-key 
Using keystore-file : client.keystore
One certificate, no chain.
Key and certificate stored.
Alias:client-key  Password:changeme







Create a truststore with the RootCA in it.

$ "$JAVA_HOME/bin/keytool" -import -alias rootca -file certificateauthority.crt -trustcacerts -keystore truststore -storepass changeme 
Owner: EMAILADDRESS=certificateauthority@myorganization.com, CN=certificateauthority, OU=IT, O=myorganization, L=Zug, ST=Zug, C=CH
Issuer: EMAILADDRESS=certificateauthority@myorganization.com, CN=certificateauthority, OU=IT, O=myorganization, L=Zug, ST=Zug, C=CH
Serial number: f5baa235f2b087a7
Valid from: Mon Aug 23 22:41:26 CEST 2010 until: Tue Aug 23 22:41:26 CEST 2011
Certificate fingerprints:
         MD5:  00:13:D2:E5:7B:9F:63:A5:88:F0:AE:69:94:44:4F:33
         SHA1: 87:2B:D7:ED:8D:D6:39:05:55:EA:7A:79:31:97:65:FE:66:DC:E3:53
         Signature algorithm name: SHA1withRSA
         Version: 3

Extensions: 

#1: ObjectId: 2.5.29.14 Criticality=false
SubjectKeyIdentifier [
KeyIdentifier [
0000: E8 3B C3 96 B5 28 0D 86   3B DC DF B1 75 A5 0C A8  .;...(..;...u...
0010: F5 FD C1 24                                        ...$
]
]

#2: ObjectId: 2.5.29.19 Criticality=false
BasicConstraints:[
  CA:true
  PathLen:2147483647
]

#3: ObjectId: 2.5.29.35 Criticality=false
AuthorityKeyIdentifier [
KeyIdentifier [
0000: E8 3B C3 96 B5 28 0D 86   3B DC DF B1 75 A5 0C A8  .;...(..;...u...
0010: F5 FD C1 24                                        ...$
]

[EMAILADDRESS=certificateauthority@myorganization.com, CN=certificateauthority, OU=IT, O=myorganization, L=Zug, ST=Zug, C=CH]
SerialNumber: [    f5baa235 f2b087a7]
]

Trust this certificate? [no]:  yes
Certificate was added to keystore



Add the server certificate to the truststore.

$ "$JAVA_HOME/bin/keytool" -import -alias server -file server.crt -trustcacerts -keystore truststore -storepass changeme 
Certificate was added to keystore


$ "$JAVA_HOME/bin/keytool" -list -keystore truststore            
Enter keystore password:  changeme

Keystore type: JKS
Keystore provider: SUN

Your keystore contains 2 entries

rootca, Aug 23, 2010, trustedCertEntry,
Certificate fingerprint (MD5): 00:13:D2:E5:7B:9F:63:A5:88:F0:AE:69:94:44:4F:33
server, Aug 23, 2010, trustedCertEntry,
Certificate fingerprint (MD5): 75:EC:46:5D:52:AD:C0:D4:9A:82:0F:1E:E3:3F:AE:01









client keystore: private key + public certificate of client(signed by public certificate of server).
client truststore: public certificate of server + certificationauthority certificate

server keystore: private key + public certificate of server
server truststore: public certificate of server(signed by certification authority) + certificationauthority certificate



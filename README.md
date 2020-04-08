# Chain Replication for Supporting High Throughput and Availability
This application is intended for supporting large-scale storage services that exhibit high throughput and availability without sacrificing strong consistency guarantees

Download <a href="https://sjsu.instructure.com/courses/1354488/files/55585055/download?wrap=1">Zookeeper Jar</a>

## Steps to Run
### Step 1: 
Start Zookeeper Server:This command will start the Zookeeper server which listens on port number 9999\
java -jar zookeeper-dev-fatjar.jar server 9999 /tmp/zookeeper/

Start Zookeeper Client:This command will establish a connection between the client and server(Run client on a different terminal)\
java -jar zookeeper-dev-fatjar.jar client -server 127.0.0.1:9999



### Step 2: 
In IntelliJ/Eclipse IDE create a new java project<br /> 
Compile this project and create its jar<br />


### Step 3: Run the replica
The command below takes the following parameters:\
i. Zookeeper server's ip address and port number\
ii. Directory on zookeeper to which our replica will join\
iii. Replica's ip address\
iv. Replica's port number

### Command:
java -cp    target/(mvn package).jar    directory.ReplicaClassName    (Zookeeper server's ip address):(port number)   /directory-name   (Replica's ip)    (Replica's port number)  

For example:\
java -cp    target/chain-java-1.0-SNAPSHOT-jar-with-dependencies.jar    edu.sjsu.cs249.chain.TailChainServer    127.0.0.1:9999  /tailchain    172.20.10.4 4588

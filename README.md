# jclient-worker 
###### March 2020

## Brief
Various java worker examples:
- Monitoring collStats, rs.status()
- Change stream study including resume token. 
- Using singleton abstract class for dynamic writeConcern settings
- Based on ScheduledExecutorService

### Example

Using Dockerfile
```bash
$ mvn clean install assembly:single 
$ docker build -f Dockerfile -t jclientdemo:1.2 .
$ docker run --rm jclientdemo:1.2 java -jar /jclient.jar '--uri=mongodb://hostname:27107,...,hostname2:27107/?replicaSet=myrsname&retryWrites=true' '--db=jclient' '--col=example' '--usecase=4'
```

Using docker-maven plugin 
```bash
$ docker run --rm -v "$(pwd)":/home -w /home maven:3-jdk-8 mvn --quiet compile exec:java -Dexec.mainClass=com.gpi.App "-Dexec.args=-u mongodb://hostname:27107,...,hostname2:27107/?authSource=admin&replicaSet=myrsname&retryWrites=true&w=majority&wtimeoutMS=5000 -d jclient -c example -uc 4"
```

Using maven 
```bash
$ compile exec:java -Dexec.mainClass=com.gpi.App "-Dexec.args=--uri 'mongodb://hostname:27107,...,hostname2:27107/?replicaSet=myrsName&retryWrites=true&w=majority&wtimeoutMS=5000' -d jclient -c example -uc 4"
```

Jar application
```bash
$ java -jar ./target/jclient-2.0.2-SNAPSHOT-jar-with-dependencies.jar --uri mongodb://hostname:27107,...,hostname2:27107/?replicaSet=myrsName&retryWrites=true -d jclient -c example -uc 4
```

### Testing environment
- Mongodb server 4.0.17
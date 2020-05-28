# jclient-worker 
###### March 2020

## Brief
Various java worker examples:
- Monitoring collStats, rs.status()
- Change stream study including resume token and multiple watchers (case 5) 
- Using singleton abstract class for dynamic writeConcern settings
- Based on ScheduledExecutorService
- Integration of insertMany from mgenerate4j (case 6)

### Example

#### Using Dockerfile
```bash
$ mvn clean install assembly:single 
$ docker build -f Dockerfile -t jclientdemo:1.2 .
$ docker run --rm jclientdemo:1.2 java -jar /jclient.jar '--uri=mongodb://hostname:27107,...,hostname2:27107/?replicaSet=myrsname&retryWrites=true' '--db=jclient' '--col=example' '--usecase=4'
```

#### Using docker-maven plugin 
```bash
$ docker run --rm -v "$(pwd)":/home -w /home maven:3-jdk-8 mvn --quiet compile exec:java -Dexec.mainClass=com.gpi.App "-Dexec.args=-u mongodb://hostname:27107,...,hostname2:27107/?authSource=admin&replicaSet=myrsname&retryWrites=true&w=majority&wtimeoutMS=5000 -d jclient -c example -uc 4"
```

#### Using maven 
```bash
$ compile exec:java -Dexec.mainClass=com.gpi.App "-Dexec.args=--uri 'mongodb://hostname:27107,...,hostname2:27107/?replicaSet=myrsName&retryWrites=true&w=majority&wtimeoutMS=5000' -d jclient -c example -uc 4"
```

####Jar application
```bash
$ java -jar ./target/jclient-2.0.2-SNAPSHOT-jar-with-dependencies.jar --uri mongodb://hostname:27107,...,hostname2:27107/?replicaSet=myrsName&retryWrites=true -d jclient -c example -uc 4
```
### Use cases

#### Generate insertMany using mgenerate4j;
```bash
$ compile exec:java -Dexec.mainClass=com.gpi.App "-Dexec.args=--uri 'mongodb://hostname:27107,...,hostname2:27107/?replicaSet=myrsName&retryWrites=true&w=majority&wtimeoutMS=5000' -d jclient -c example -uc 6 -m offer.json"
```

#### Create dual watcher ( watcher 1: range[0-9999], watcher 2: range[1000-2000]) 
```bash
$ compile exec:java -Dexec.mainClass=com.gpi.App "-Dexec.args=--uri 'mongodb://hostname:27107,...,hostname2:27107/?replicaSet=myrsname&retryWrites=true&w=majority&wtimeoutMS=5000' -d referential -c offer -uc 5"
```
Dummy manual inserts requires offers with an effective date
```bash
$ mongo --host myrsname/hostname:27107,...,hostname2:27107/referential
> db.offer.createIndex({date:1},{ expireAfterSeconds: 5 })
> for (i=0;i<50;i++){db.offer.insertOne({_id:i,hello:"world",date:ISODate()}); db.offer.insertOne({_id:i+1000,hello:"world",date:ISODate()});}
```

### Usage
```bash
usage: jClient [-c <arg>] [-cu <arg>] [-d <arg>] [-m <arg>] [-p <arg>]
       [-s] -u <arg> [-uc <arg>]
Option arguments may be in any order
 -c,--col <arg>        Collection name. Default is jcol
 -cu,--cleanup <arg>   Option to drop working collection. Default is false
 -d,--db <arg>         Database name. Default is jdb
 -m,--model <arg>      Optional file name for template json model used by
                       mgenerate. Default is null
 -p,--periodic <arg>   Mode single or peridic
 -s,--ssl              Enable TLS/SSL host certification true/false
 -u,--uri <arg>        Mongo connection string ie,
                       mongodb://localhost:27017
 -uc,--usecase <arg>   Use case: 1 - single collection stats 2 - RS status
                       Use case 2 - periodic rs.status()
                       Use case 3 - periodic rs.status() &
                       runCommand({collstats:mycol})
                       Use case 4 - periodic rs.status() &
                       runCommand({collstats:mycol})
                       Use case 5 - run dual watchers and manage pending
                       status
                       Use case 6 - run (once) an insertMany generating
                       documents against json template
```

### Testing environment
- Mongodb server 4.0.17
FROM java:8
# WORKDIR /
ADD target/jclient-2.0.2-SNAPSHOT-jar-with-dependencies.jar jclient.jar
# ENTRYPOINT ["java", "-jar", "jclient.jar"]
# CMD ["--uri=mongodb://localhost:27017,localhost:27018,localhost:27019,localhost:27020/?replicaSet=rsjunk&retryWrites=true", "--db=jclient", "--col=01", "--usecase=4"]
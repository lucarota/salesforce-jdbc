source ~/java8
export MAVEN_BASEDIR=
mvn clean install -P single-jar && cp -f ./sf-jdbc-driver/target/sf-jdbc-driver-*-SNAPSHOT-jar-with-dependencies.jar ./deliverables/

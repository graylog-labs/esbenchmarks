# Benchmarks for Graylog Bulk Indexing implementations

This is a repository containing benchmarks for different Graylog Bulk Indexing implementations. Currently tested are:

  * Using the Elasticsearch Tribe Node client (from Graylog 2.2.3) **(node submodule)**
  * Using the new HTTP Client based on Jest (from Graylog 2.3.0-alpha.3) **(http submodule)**
  * Using the new HTTP Client based on Jest (from Graylog 2.3.0-alpha.4-SNAPSHOT/master) **(httpmaster submodule)**
 

## How to run

These benchmarks are based on jmh. Each module generates a separate jar file that can be run. There is no uberjar file generated because of the class clashes that would happen due to the different versions of the same Graylog server jars used in each module. 

Start by running `mvn package` to compile all three modules. For the `httpmaster` module you also need to do a `mvn install` in a recent `master` snapshot of the Graylog server repository.

To run one of those jars afterwards you can do:

 * for the `http` module: `java -Dmessages=10000 -Des.uri=http://localhost:9200 -jar http/target/benchmarks.jar`
 * for the `httpmaster` module: `java -Dmessages=10000 -Des.uri=http://localhost:9200 -jar httpmaster/target/benchmarks.jar`
 * for the `node` module: `java -Dmessages=10000 -Des.host=localhost:9300 -Des.cluster=yourclustername node/target/benchmarks.jar`

For more information how to change the runtime parameters of the benchmarks run any jar with the `-h` parameter.


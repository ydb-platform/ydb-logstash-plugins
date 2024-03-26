### Building the YDB Logstash plugins

#### Requirements

* Java 11 or newer
* Installed [Logstash](https://www.elastic.co/guide/en/logstash/current/installing-logstash.html)
* Installed [Gradle](https://gradle.org/)

#### Configure and build

1. Clone project to working directory
   ``` git clone git@github.com:ydb-platform/ydb-logstash-plugins.git <work-dir> ```
2. Add gradle config with path to Logstash installation
   ``` echo "LOGSTASH_CORE_PATH=<logstash-path>/logstash-core" > <work-dir>/gradle.properties ```
3. Build plugins set
   ``` cd <work-dir> && ./gradlew gem ```



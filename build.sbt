name := "commerce_platform_data_analysis"

version := "0.1"

scalaVersion := "2.10.7"

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/content/repositories/releases/",
  "alibaba" at "http://maven.aliyun.com/nexus/content/groups/public/"
)

// Spark-Core
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0-cdh5.13.2" exclude("javax.servlet", "*") exclude("com.fasterxml.jackson.core", "*") exclude("com.fasterxml.jackson.module", "jackson-module-scala_2.10")

// Spark-Sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0-cdh5.13.2" exclude("javax.servlet", "*")


// 解析IP地域
libraryDependencies += "com.maxmind.geoip2" % "geoip2" % "2.12.0" exclude("com.fasterxml.jackson.core", "*")

// 由于Spark和Geoip2都依赖jackson，选择两者都兼容的版本
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.7.3"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.7.3"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.7.3"

// 解析Usr-Agent
libraryDependencies += "cz.mallat.uasparser" % "uasparser" % "0.6.2"

// MySQL - JDBC
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.46"


//libraryDependencies += "org.apache.hive" % "hive-exec" % "1.1.0-cdh5.13.2"


// 集成Hbase
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.8"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.8"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.8"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0-cdh5.13.2"

name := "commerce_platform_data_analysis"

version := "0.1"

scalaVersion := "2.10.6"


resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "alibaba" at "http://maven.aliyun.com/nexus/content/groups/public/",
  "Jcn Repository" at "http://nexus.jcndev.com/nexus/content/groups/public",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.io"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0-cdh5.13.2" exclude("com.fasterxml.jackson.core", "*") exclude("com.fasterxml.jackson.module", "jackson-module-scala_2.10")

libraryDependencies += "com.maxmind.geoip2" % "geoip2" % "2.12.0" exclude("com.fasterxml.jackson.core", "*")

// 由于Spark和Geoip2都依赖jackson，选择两者都兼容的版本
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.7.3"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.7.3"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.7.3"


libraryDependencies += "cz.mallat.uasparser" % "uasparser" % "0.6.2"


libraryDependencies += "org.apache.hive" % "hive-exec" % "1.1.0-cdh5.13.2"


name := "sparktest"

version := "1.0"

scalaVersion := "2.11.8"

val overrideScalaVersion = "2.11.8"
val sparkVersion = "2.0.0"
val sparkXMLVersion = "0.3.3"
val sparkCsvVersion = "1.4.0"
val sparkElasticVersion = "2.3.4"
val sscKafkaVersion = "1.6.2"
val sparkMongoVersion = "1.0.0"
val sparkCassandraVersion = "1.6.0"

//Override Scala Version to the above 2.11.8 version
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark"      %%  "spark-core"      %   sparkVersion  exclude("jline", "2.12"),
  "org.apache.spark"      %% "spark-sql"        % sparkVersion excludeAll(ExclusionRule(organization = "jline"),ExclusionRule("name","2.12")),
  "org.apache.spark"      %% "spark-hive"       % sparkVersion,
  "org.apache.spark"      %% "spark-yarn"       % sparkVersion,
  "com.databricks"        %% "spark-xml"        % sparkXMLVersion,
  "com.databricks"        %% "spark-csv"        % sparkCsvVersion,
  "org.apache.spark"      %% "spark-graphx"     % sparkVersion,
  "org.apache.spark"      %% "spark-catalyst"   % sparkVersion,
  "org.apache.spark"      %% "spark-streaming"  % sparkVersion,
  "org.apache.spark"      %% "spark-mllib"      % sparkVersion,
  //  "com.101tec"           % "zkclient"         % "0.9",
  "org.elasticsearch"     %% "elasticsearch-spark"        %     sparkElasticVersion,
  "org.apache.spark"      %% "spark-streaming-kafka"     % sscKafkaVersion,
  "org.mongodb.spark"      % "mongo-spark-connector_2.11" %  sparkMongoVersion,
     
  // breeze 
  "org.scalanlp" %% "breeze" % "0.12" ,
  "org.scalanlp" %% "breeze-natives" % "0.12" ,
  "org.scalanlp" %% "breeze-viz" % "0.12"
  //"com.stratio.datasource" % "spark-mongodb_2.10"         % "0.11.1"

  // Adding this directly as part of Build.sbt throws Guava Version incompatability issues.
  // Please look my Spark Cassandra Guava Shade Project and use that Jar directly.
  //"com.datastax.spark"     % "spark-cassandra-connector_2.11" % sparkCassandraVersion
)

/*
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>  
    {  
        case PathList("org", "slf4j", xs @ _*)         => MergeStrategy.first  
        case PathList(ps @ _*) if ps.last endsWith "axiom.xml" => MergeStrategy.filterDistinctLines  
        case PathList(ps @ _*) if ps.last endsWith "Log$Logger.class" => MergeStrategy.first  
        case PathList(ps @ _*) if ps.last endsWith "ILoggerFactory.class" => MergeStrategy.first  
        case x => old(x)  
    }  
}  
*/

//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")

resolvers ++= Seq(
    //"Nexus osc" at "http://maven.oschina.net/content/groups/public/",
    //"Nexus osc thirdparty" at "http://maven.oschina.net/content/repositories/thirdparty/",
    "typesafe" at "http://repo.typesafe.com/typesafe/ivy-releases/, [organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext    ], bootOnly",
    "typesafe2" at "http://repo.typesafe.com/typesafe/releases/",
    "sbt-plugin" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/",
    "sonatype" at "http://oss.sonatype.org/content/repositories/snapshots",
    "uk_maven" at "http://uk.maven.org/maven2/",
    "ibibli" at "http://mirrors.ibiblio.org/maven2/",
    "repo2" at "http://repo2.maven.org/maven2/"
  //"Local Maven Repository" at "file://Users/yuanpingzhou/.m2/repository"
  // other resolvers here
    // if you want to use snapshot builds (currently 0.12-SNAPSHOT), use this.
      //"Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
      //"Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

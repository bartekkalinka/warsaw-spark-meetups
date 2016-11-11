name := "handle-allocated-containers"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal
resolvers += "Spark 2.0.0 RC2" at "https://repository.apache.org/content/repositories/orgapachespark-1189"

//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0-RC4" % "test"


    
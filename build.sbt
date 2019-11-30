name := "Sparking"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.3.1",
  "com.typesafe" % "config" % "1.3.2",
"org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.novocode" % "junit-interface" % "0.11" % "test"
)
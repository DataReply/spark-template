A scala/sbt template for a standalone Spark application.

* The name of your application should be modified in 2 places:
  - in project/build.scala => settings.name
  - (OPTIONAL) in src/main/scala/main.scala => the name of the object

* The settings for the spark cluster can be modified in resources/application.conf
  - the hostname of the spark master
  - the memory for each spark worker
  - the path to the spark installation

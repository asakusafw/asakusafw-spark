# Asakusa Framework on Spark Project
Asakusa on Spark provides facilities that make [Asakusa](https://github.com/asakusafw/asakusafw) batch applications run on [Apache Spark](https://spark.apache.org/) clusters.

This project includes the followings:

* [Asakusa DSL compiler](https://github.com/asakusafw/asakusafw-compiler) for Spark
* Asakusa runtime libraries for Spark
* Asakusa on Spark [Gradle](http://gradle.org/) plug-in

## How to build
```sh
mvn clean package
```

## How to import projects into Eclipse
```sh
mvn install eclipse:eclipse -DskipTests
```
And then import projects from Eclipse

## How to build Gradle Plugin
```sh
cd gradle
./gradlew clean build
```

## Resources
* Document (ja): [Asakusa on Spark](http://docs.asakusafw.com/preview/ja/html/asakusa-on-spark/index.html) (Developer Preview)

## License
* [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)


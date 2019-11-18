# Asakusa on Spark
Asakusa on Spark provides facilities that make [Asakusa](https://github.com/asakusafw/asakusafw) batch applications run on [Apache Spark](https://spark.apache.org/) clusters.

This project includes the followings:

* [Asakusa DSL compiler](https://github.com/asakusafw/asakusafw-compiler) for Spark
* Asakusa runtime libraries for Spark
* Asakusa on Spark [Gradle](http://gradle.org/) plug-in

## How to build
```sh
./mvnw clean package
```

## How to import projects into Eclipse
```sh
./mvnw install eclipse:eclipse -DskipTests
```
And then import projects from Eclipse

## How to build Gradle Plugin

```sh
cd gradle
./gradlew clean build [install] [-PmavenLocal]
```

## Referred Projects
* [Asakusa Framework Core](https://github.com/asakusafw/asakusafw)
* [Asakusa Framework Language Toolset](https://github.com/asakusafw/asakusafw-compiler)
* [Asakusa Framework Documentation](https://github.com/asakusafw/asakusafw-documentation)

## Resources
* [Asakusa on Spark Documentation (ja)](https://docs.asakusafw.com/asakusa-on-spark/)

## License
* [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

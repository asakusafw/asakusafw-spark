==========
Changelogs
==========

Release 0.2.1
=============

Nov 19, 2015

asakusafw-compiler
------------------

* [ :pr-compiler:`15` ] - Fix typo.
* [ :pr-compiler:`16` ] - Remove redundant entries in pom.xml.
* [ :pr-compiler:`17` ] - Bump up Asakusa Framework version to 0.7.5.

asakusafw-spark
---------------

* [ :pr-spark:`47` ] - Extract zipBroadcasts() method to UsingBroadcasts trait and some refactoring.
* [ :pr-spark:`48` ] - Use Context traits for each compilers instead of SparkClientCompiler.Context.
* [ :pr-spark:`49` ] - Add mvnw files.
* [ :pr-spark:`50` ] - Refactor FragmentGraphBuilder not to depend on SubPlanDriver.
* [ :pr-spark:`51` ] - Ruese SparkContext in each specs.
* [ :pr-spark:`52` ] - Bump Spark version to 1.5.1.
* [ :pr-spark:`53` ] - Add some files to ignore.
* [ :pr-spark:`54` ] - Refactor specs.
* [ :pr-spark:`55` ] - Use provided ExecutionContext.
* [ :pr-spark:`56` ] - Refactor specs.
* [ :pr-spark:`57` ] - Refactor specs.
* [ :pr-spark:`58` ] - Refactor Branching not to depend on SubPlanDriver.
* [ :pr-spark:`59` ] - Rename DriverLabel to LabelField.
* [ :pr-spark:`60` ] - Fix a test for AggregateDriverClassBuilder with empty grouping.
* [ :pr-spark:`61` ] - Refactor ShuffleKey to use val not var.
* [ :pr-spark:`62` ] - Fix zipPartitions / confluent / smcogroup to handle empty rdds.
* [ :pr-spark:`63` ] - Refactor tests to simplify functions.
* [ :pr-spark:`64` ] - Use sc.union() instead of UnionRDD directly.
* [ :pr-spark:`65` ] - Split tests for Input/Output and some refactoring.
* [ :pr-spark:`66` ] - Fix broadcast-joins to be able to handle core.empty master.
* [ :pr-spark:`67` ] - Revise assertions of compiler project.
* [ :pr-spark:`68` ] - Bump Spark version to 1.5.2.
* [ :pr-spark:`69` ] - Fix call-site for InputDriver.
* [ :pr-spark:`70` ] - Add a configuration `spark.kryo.referenceTracking` to SparkForAll.
* [ :pr-spark:`71` ] - Make idioms utility objects.
* [ :pr-spark:`72` ] - Initialize operator instance layzily.
* [ :pr-spark:`73` ] - Refine @transient.
* [ :pr-spark:`74` ] - Fix behavior of Iterator from OutputFragment.iterator.
* [ :pr-spark:`75` ] - Bump Scala version to 2.10.6.
* [ :pr-spark:`76` ] - Add eclipse preference file for Scala IDE.
* [ :pr-spark:`77` ] - Add scala-2.11 profile.
* [ :pr-spark:`78` ] - Modify ClassBuilder.interfaceTypes to be able to be overridden.
* [ :pr-spark:`79` ] - Bump up Asakusa Framework version to 0.7.5.

Release 0.2.0
=============

Sep 15, 2015

asakusafw-compiler
------------------

* [ :pr-compiler:`13` ] - Fix typo in documentation comments.

asakusafw-spark
---------------

* [ :pr-spark:`39` ] - Use value-class for implicit conversion.
* [ :pr-spark:`40` ] - Bump Spark version to 1.5.0.
* [ :pr-spark:`41` ] - Avoid deprecated method.
* [ :pr-spark:`44` ] - Fix typo in documentation comments.

Release 0.1.2
=============

Sep 10, 2015

This version is only fixed release problem on version 0.1.1.

Release 0.1.1
=============

Aug 25, 2015

asakusafw-compiler
------------------

* [ :pr-compiler:`2` ] - Change referred asakusafw version to `0.7.3-hadoop2`.
* [ :pr-compiler:`3` ] - Change pom.xml to use 2 space indents.
* [ :pr-compiler:`4` ] - Fix test cases on Windows.
* [ :pr-compiler:`5` ] - Revise #4 for some test cases and environments.
* [ :pr-compiler:`6` ] - Use `Objects` utility methods to improve readability.
* [ :pr-compiler:`7` ] - Put batch arguments into Direct I/O input splits.
* [ :pr-compiler:`8` ] - Bump up Asakusa Framework version to 0.7.4.
* [ :pr-compiler:`9` ] - Suppress checkstyle warnings about javadoc style.
* [ :pr-compiler:`10` ] - Custom Report API implementations are now available.

asakusafw-spark
---------------

* [ :pr-spark:`1` ] - Trivial document fix
* [ :pr-spark:`2` ] - Improve backward compatibility of Gradle versions.
* [ :pr-spark:`3` ] - Exclude asm:asm.
* [ :pr-spark:`4` ] - Use 'asakusafw-lang.version' instead of 'asakusafw-bridge.version'.
* [ :pr-spark:`6` ] - Inherit component versions from parent pom.xml in Gradle plug-ins.
* [ :pr-spark:`7` ] - Add scalastyle check.
* [ :pr-spark:`8` ] - Instantiate Configuration with loadDefault = false in Serializer.
* [ :pr-spark:`9` ] - Format scalastyle-config.xml to use 2 spaces for indent instead of tab.
* [ :pr-spark:`10` ] - Refactor PreparingKey to split `shuffleKey` method for each output.
* [ :pr-spark:`11` ] - Extract class builders.
* [ :pr-spark:`12` ] - Refactor Branching.
* [ :pr-spark:`13` ] - Change pom.xml to use 2 space indents.
* [ :pr-spark:`14` ] - Replace docs URL with `docs.asakusafw.com`.
* [ :pr-spark:`15` ] - Rename asakusa-spark-compiler-core to asakusa-spark-compiler.
* [ :pr-spark:`16` ] - Replace docs URL with `docs.asakusafw.com`.
* [ :pr-spark:`17` ] - Rename FragmentTreeBuilder to FragmentGraphBuilder.
* [ :pr-spark:`18` ] - Extract SparkClient class builder.
* [ :pr-spark:`19` ] - Introduce AugmentedCompilerOptions.
* [ :pr-spark:`20` ] - Introduce SparkClientCompiler.Context.
* [ :pr-spark:`21` ] - Rename OperatorType.MapType to ExtractType.
* [ :pr-spark:`22` ] - Remove OperatorInfo.
* [ :pr-spark:`23` ] - Add license header for a new file.
* [ :pr-spark:`24` ] - Add Scala idioms.
* [ :pr-spark:`25` ] - Follow-up #24.
* [ :pr-spark:`26` ] - Extract Instantiator's.
* [ :pr-spark:`27` ] - Suppress "unchecked" warnings.
* [ :pr-spark:`28` ] - Suppress Scala version incompatible warnings.
* [ :pr-spark:`29` ] - Bump Spark version to 1.4.1.
* [ :pr-spark:`30` ] - Refactor and add SparkIdioms.
* [ :pr-spark:`31` ] - Use `Objects` utility methods to improve readability.
* [ :pr-spark:`32` ] - Bump up asakusafw-lang version.
* [ :pr-spark:`33` ] - Bump up Asakusa Framework version to 0.7.4.
* [ :pr-spark:`34` ] - Remove duplicated classes.
* [ :pr-spark:`35` ] - Use FileMapListBuffer to prevent OOM.
* [ :pr-spark:`36` ] - Modify variable names in spark bootstrap script.
* [ :pr-spark:`37` ] - 0.1.1 Documents

Release 0.1.0
=============

Jun 26, 2015

* The first developer preview release of Asakusa on Spark.


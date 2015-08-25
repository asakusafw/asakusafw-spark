==========
Changelogs
==========

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

Release 0.1.0
=============

Jun 26, 2015

* The first developer preview release of Asakusa on Spark.


============================
Asakusa on Sparkの最適化設定
============================

この文書では、Asakusa on Sparkのバッチアプリケーション実行時に設定可能な最適化設定について説明します。

..  warning::
    現時点では、この文書で説明する Asakusa on Spark の機能は **Developer Preview** として公開しています。
    
    * プロダクション環境での利用は現時点では非推奨です。
    * 本機能は今後、予告なしに仕様を変更する可能性があります。
    * 本機能は今後、予告なしに対応プラットフォームを変更する可能性があります。

設定ファイル
============

Asakusa on Sparkに関するバッチアプリケーション実行時のパラメータは、 :file:`$ASAKUSA_HOME/spark/conf/spark.properties` に記述します。このファイルは、Asakusa on Spark Gradle Pluginを有効にしてデプロイメントアーカイブを作成した場合にのみ含まれています。

このファイルに設定した内容はSpark向けのバッチアプリケーションの設定として使用され、バッチアプリケーション実行時の動作に影響を与えます。

設定ファイルはJavaのプロパティファイルのフォーマットと同様です。

**spark.properties**

..  code-block:: properties
    
    ## the number of parallel tasks of each Asakusa stage
    com.asakusafw.spark.parallelism=40

設定項目
========

:file:`$ASAKUSA_HOME/spark/conf/spark.properties` に設定可能な項目は以下の通りです。

``com.asakusafw.spark.parallelism``
    バッチアプリケーション実行時に、ステージごとの標準的なタスク分割数を指定します。

    ステージの特性（推定データサイズや処理内容）によって、この分割数を元に実際のタスク分割数が決定されます。

    標準的には、SparkのExecutorに割り当てた全コア数の1〜4倍程度を指定します。
    
    このプロパティが設定されていない場合、 ``spark.default.parallelism`` の値を代わりに利用します。いずれのプロパティも設定されていない場合、下記の既定値を利用します。

    既定値: ``2``

``com.asakusafw.spark.parallelism.scale.small``
    コンパイラが「比較的小さなデータ」と判断したステージのタスク分割数を標準からの割合で指定します。

    標準のタスク分割数については ``com.asakusafw.spark.parallelism`` で設定できます。また、データサイズの閾値についてはコンパイラプロパティの ``spark.parallelism.limit.small`` で設定できます。

    既定値: ``0.5``

``com.asakusafw.spark.parallelism.scale.large``
    コンパイラが「比較的大きなデータ」と判断したステージのタスク分割数を標準からの割合で指定します。

    標準のタスク分割数については ``com.asakusafw.spark.parallelism`` で設定できます。また、データサイズの閾値についてはコンパイラプロパティの ``spark.parallelism.limit.large`` で設定できます。

    既定値: ``2.0``

``com.asakusafw.spark.parallelism.scale.huge``
    コンパイラが「巨大なデータ」と判断したステージのタスク分割数を標準からの割合で指定します。

    標準のタスク分割数については ``com.asakusafw.spark.parallelism`` で設定できます。また、データサイズの閾値についてはコンパイラプロパティの ``spark.parallelism.limit.huge`` で設定できます。

    既定値: ``4.0``

..  seealso::
    コンパイラプロパティについては、 :doc:`reference` を参照してください。




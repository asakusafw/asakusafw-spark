============================
Asakusa on Sparkの最適化設定
============================

この文書では、Asakusa on Sparkのバッチアプリケーション実行時の最適化設定について説明します。

..  warning::
    現時点では、この文書で説明する Asakusa on Spark の機能は **Developer Preview** として公開しています。
    
    * プロダクション環境での利用は現時点では非推奨です。
    * 本機能は今後、予告なしに仕様を変更する可能性があります。
    * 本機能は今後、予告なしに対応プラットフォームを変更する可能性があります。

設定方法
========

Asakusa on Sparkのバッチアプリケーション実行時の設定は、 `設定ファイル`_ を使う方法と `環境変数`_ を使う方法があります。

設定ファイル
------------

Asakusa on Sparkに関するバッチアプリケーション実行時のパラメータは、 :file:`$ASAKUSA_HOME/spark/conf/spark.properties` に記述します。
このファイルは、Asakusa on Spark Gradle Pluginを有効にしてデプロイメントアーカイブを作成した場合にのみ含まれています。

このファイルに設定した内容はSpark向けのバッチアプリケーションの設定として使用され、バッチアプリケーション実行時の動作に影響を与えます。

設定ファイルはJavaのプロパティファイルのフォーマットと同様です。以下は ``spark.properties`` の設定例です。

**spark.properties**

..  code-block:: properties
    
    ## the number of parallel tasks of each Asakusa stage
    com.asakusafw.spark.parallelism=40
    
    ## the number of records of the in-memory buffer for each output fragment
    com.asakusafw.spark.fragment.bufferSize=256

環境変数
--------

Asakusa on Sparkに関するバッチアプリケーション実行時のパラメータは、環境変数 ``ASAKUSA_SPARK_APP_CONF`` に設定することもできます。

環境変数 ``ASAKUSA_SPARK_APP_CONF`` の値には ``--engine-conf <key>=<value>`` という形式でパラメータを設定します。

以下は環境変数の設定例です。

..  code-block:: sh
    
    ASAKUSA_SPARK_APP_CONF='--engine-conf com.asakusafw.spark.parallelism=40 --engine-conf com.asakusafw.spark.fragment.bufferSize=256'

設定ファイルと環境変数で同じプロパティが設定されていた場合、環境変数の値が利用されます。

..  hint::
    環境変数による設定は、バッチアプリケーションごとに設定を変更したい場合に便利です。
    
..  attention::
    :program:`yaess-batch.sh` などのYAESSコマンドを実行する環境と、Sparkを実行する環境が異なる場合（例えばYAESSのSSH機能を利用している場合）に、
    YAESSコマンドを実行する環境の環境変数がSparkを実行する環境に受け渡されないことがある点に注意してください。
    
    YAESSではYAESSコマンドを実行する環境の環境変数をYAESSのジョブ実行先に受け渡すための機能がいくつか用意されているので、それらの機能を利用することを推奨します。
    詳しくは :asakusafw:`YAESSユーザガイド <yaess/user-guide.html>` などを参照してください。

設定項目
========

Asakusa on Sparkのバッチアプリケーション実行時の設定項目は以下の通りです。

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

``com.asakusafw.spark.fragment.bufferSize``
    演算子の出力に結果 ( ``Result`` [#]_ ) 型を使用する場合に、``Result型`` に追加するデータモデルオブジェクトをメモリ上に保持する個数を指定します。
    
    演算子の処理中に ``Result`` に追加したデータモデルオブジェクトの個数がこのプロパティに設定した値を超えた時点で、 ``Result`` の内容をファイル上のバッファに退避します。

    このプロパティを設定しない、または負の値を指定した場合、演算子の処理が終了するまで ``Result`` に追加したすべてのデータモデルオブジェクトはメモリ上に保持されます。
    
    既定値: ``-1``

..  [#] :asakusafw-javadoc:`com.asakusafw.runtime.core.Result` 演算子の出力となるデータモデルオブジェクトを保持します。 ``add`` メソッドにより複数のオブジェクトを追加することができます。

..  hint::
    ある演算子の出力サイズが大きくメモリ不足エラーが発生するような場合に、このプロパティを設定することで問題を回避できる可能性があります。

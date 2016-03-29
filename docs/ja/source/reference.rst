=============================
Asakusa on Spark リファレンス
=============================

この文書では、Asakusa on Sparkが提供するGradle PluginやDSLコンパイラの設定、およびバッチアプリケーション実行時の設定などについて説明します。

..  _`Apache Spark`: http://spark.apache.org/

Asakusa on Spark Gradle Plugin リファレンス
===========================================

Asakusa on Spark Gradle Pluginが提供する機能とインターフェースについて個々に解説します。

プラグイン
----------

``asakusafw-spark``
    アプリケーションプロジェクトで、Asakusa on Sparkのさまざまな機能を有効にする [#]_ 。

    このプラグインは ``asakusafw`` プラグインや ``asakusafw-organizer`` プラグインを拡張するように作られているため、それぞれのプラグインも併せて有効にする必要がある（ ``apply plugin: 'asakusafw-spark'`` だけではほとんどの機能を利用できません）。

..  [#] :asakusa-spark-gradle-groovydoc:`com.asakusafw.spark.gradle.plugins.AsakusafwSparkPlugin`

タスク
------

``sparkCompileBatchapps``
    Asakusa DSL Compiler for Sparkを利用してDSLをコンパイルする [#]_ 。

    ``asakusafw`` プラグインが有効である場合にのみ利用可能。

``attachComponentSpark``
    デプロイメントアーカイブにSpark向けのバッチアプリケーションを実行するためのコンポーネントを追加する。

    ``asakusafw-organizer`` プラグインが有効である場合にのみ利用可能。

    ``asakusafwOrganizer.spark.enabled`` に ``true`` が指定されている場合、自動的に有効になる。

``attachSparkBatchapps``
    デプロイメントアーカイブに ``sparkCompileBatchapps`` でコンパイルした結果を含める。

    ``asakusafw`` , ``asakusafw-organizer`` の両プラグインがいずれも有効である場合にのみ利用可能。

    ``asakusafwOrganizer.batchapps.enabled`` に ``true`` が指定されている場合、自動的に有効になる。

..  [#] :asakusafw-sdk-gradle-groovydoc:`com.asakusafw.gradle.tasks.AsakusaCompileTask`

タスク拡張
----------

``assemble``
    デプロイメントアーカイブを生成する。

    ``asakuafw-spark`` と ``asakusafw-organizer`` プラグインがいずれも有効である場合、 ``sparkCompileBatchapps`` が依存関係に追加される。

``attachAssemble_<profile名>``
    対象のプロファイルのデプロイメントアーカイブに必要なコンポーネントを追加する。

    ``asakuafw-spark`` と ``asakusafw-organizer`` プラグインがいずれも有効である場合、 ``attachComponentSpark`` や ``attachSparkBatchapps`` のうち有効であるものが依存関係に追加される。

    このタスクはデプロイメントアーカイブを生成する ``assembleAsakusafw`` の依存先になっているため、このタスクの依存先に上記のタスクを追加することで、デプロイメントアーカイブに必要なコンポーネントを追加できるようになっている。

規約プロパティ拡張
------------------

Batch Application Plugin ( ``asakusafw`` ) への拡張
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Asakusa on Spark Gradle Pluginは Batch Application Plugin に対して Asakusa on Sparkのビルド設定を行うための規約プロパティを追加します。この規約プロパティは、 ``asakusafw`` ブロック内の参照名 ``spark`` でアクセスできます [#]_ 。

以下、 ``build.gradle`` の設定例です。

**build.gradle**

..  code-block:: groovy
    
    asakusafw {
        spark {
            include 'com.example.batch.*'
            compilerProperties += ['spark.input.direct':'false']
        }

この規約オブジェクトは以下のプロパティを持ちます。

``spark.outputDirectory``
    コンパイラの出力先を指定する。

    文字列や ``java.io.File`` などで指定し、相対パスが指定された場合にはプロジェクトからの相対パスとして取り扱う。

    既定値: ``"$buildDir/spark-batchapps"``

``spark.include``
    コンパイルの対象に含めるバッチクラス名のパターンを指定する。

    バッチクラス名には ``*`` でワイルドカードを含めることが可能。

    また、バッチクラス名のリストを指定した場合、それらのパターンのいずれかにマッチしたバッチクラスのみをコンパイルの対象に含める。

    既定値: ``null`` (すべて)

``spark.exclude``
    コンパイルの対象から除外するバッチクラス名のパターンを指定する。

    バッチクラス名には ``*`` でワイルドカードを含めることが可能。

    また、バッチクラス名のリストを指定した場合、それらのパターンのいずれかにマッチしたバッチクラスをコンパイルの対象から除外する。

    ``include`` と ``exclude`` がいずれも指定された場合、 ``exclude`` のパターンを優先して取り扱う。

    既定値: ``null`` (除外しない)

``spark.runtimeWorkingDirectory``
    実行時のテンポラリワーキングディレクトリのパスを指定する。

    パスにはURIやカレントワーキングディレクトリからの相対パスを指定可能。

    未指定の場合、コンパイラの標準設定である「 ``target/hadoopwork`` 」を利用する。

    既定値: ``null`` (コンパイラの標準設定を利用する)

``spark.compilerProperties``
    `コンパイラプロパティ`_ （コンパイラのオプション設定）を追加する。

    この値はマップ型 ( ``java.util.Map`` ) であるため、プロパティのキーと値をマップのキーと値として追加可能。

    既定値: (Spark向けのコンパイルに必要な最低限のもの)

``spark.batchIdPrefix``
    Spark向けのバッチアプリケーションに付与するバッチIDの接頭辞を指定する。

    文字列を設定すると、それぞれのバッチアプリケーションは「 ``<接頭辞><本来のバッチID>`` 」というバッチIDに強制的に変更される。

    空文字や ``null`` を指定した場合、本来のバッチIDをそのまま利用するが、他のコンパイラが生成したバッチアプリケーションと同じバッチIDのバッチアプリケーションを生成した場合、アプリケーションが正しく動作しなくなる。

    既定値: ``"spark."``

``spark.failOnError``
    Spark向けのコンパイルを行う際に、コンパイルエラーが発生したら即座にコンパイルを停止するかどうかを選択する。

    コンパイルエラーが発生した際に、 ``true`` を指定した場合にはコンパイルをすぐに停止し、 ``false`` を指定した場合には最後までコンパイルを実施する。

    既定値: ``true`` (即座にコンパイルを停止する)

..  [#] これらのプロパティは規約オブジェクト :asakusafw-sdk-gradle-groovydoc:`com.asakusafw.gradle.plugins.AsakusafwCompilerExtension` が提供します。

Framework Organizer Plugin ( ``asakusafwOrganizer`` ) への拡張
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Asakusa on Spark Gradle Plugin は Framework Organizer Plugin に対して Asakusa on Sparkのビルド設定を行うための規約プロパティを追加します。この規約プロパティは、 ``asakusafwOrganizer`` ブロック内の参照名 ``spark`` でアクセスできます [#]_ 。

この規約オブジェクトは以下のプロパティを持ちます。

``spark.enabled``
    デプロイメントアーカイブにSpark向けのバッチアプリケーションを実行するためのコンポーネントを追加するかどうかを指定する (各プロファイルのデフォルト値)。

    ``true`` を指定した場合にはコンポーネントを追加し、 ``false`` を指定した場合には追加しない。

    既定値: ``true`` (コンポーネントを追加する)

``<profile>.spark.enabled``
    対象のプロファイルに対し、デプロイメントアーカイブにSpark向けのバッチアプリケーションを実行するためのコンポーネントを追加するかどうかを指定する。

    前述の ``spark.enabled`` と同様だが、こちらはプロファイルごとに指定できる。

    既定値: ``asakusafwOrganizer.spark.enabled`` (全体のデフォルト値を利用する)

..  [#] これらのプロパティは規約オブジェクト :asakusa-spark-gradle-groovydoc:`com.asakusafw.spark.gradle.plugins.AsakusafwOrganizerSparkExtension` が提供します。

コマンドラインオプション
------------------------

:program:`sparkCompileBatchapps` タスクを指定して :program:`gradlew` コマンドを実行する際に、 ``sparkCompileBatchapps --update <バッチクラス名>`` と指定することで、指定したバッチクラス名のみをバッチコンパイルすることができます。

また、バッチクラス名の文字列には ``*`` をワイルドカードとして使用することもできます。

以下の例では、パッケージ名に ``com.example.target.batch`` を含むバッチクラスのみをバッチコンパイルしてデプロイメントアーカイブを作成しています。

..  code-block:: sh

    ./gradlew sparkCompileBatchapps --update com.example.target.batch.* assemble

そのほか、 :program:`sparkCompileBatchapps` タスクは :program:`gradlew` コマンド実行時に以下のコマンドライン引数を指定することができます。

..  program:: sparkCompileBatchapps

..  option:: --compiler-properties <k1=v1[,k2=v2[,...]]>

    追加のコンパイラプロパティを指定する。

    規約プロパティ ``asakusafw.spark.compilerProperties`` で設定したものと同じキーを指定した場合、それらを上書きする。

..  option:: --batch-id-prefix <prefix.>

    生成するバッチアプリケーションに、指定のバッチID接頭辞を付与する。

    規約プロパティ ``asakusafw.spark.batchIdPrefix`` の設定を上書きする。

..  option:: --fail-on-error <"true"|"false">

    コンパイルエラー発生時に即座にコンパイル処理を停止するかどうか。

    規約プロパティ ``asakusafw.spark.failOnError`` の設定を上書きする。

..  option:: --update <batch-class-name-pattern>

    指定のバッチクラスだけをコンパイルする (指定したもの以外はそのまま残る)。

    規約プロパティ ``asakusafw.spark.{in,ex}clude`` と同様にワイルドカードを利用可能。

    このオプションが設定された場合、規約プロパティ ``asakusafw.spark.{in,ex}clude`` の設定は無視する。

Asakusa DSL Compiler for Spark リファレンス
===========================================

コンパイラプロパティ
--------------------

Asakusa DSL Compiler for Sparkで利用可能なコンパイラプロパティについて説明します。これらの設定方法については、 `Batch Application Plugin ( asakusafw ) への拡張`_ の ``spark.compilerProperties`` の項を参照してください。

``inspection.dsl``
    DSLの構造を可視化するためのファイル( ``etc/inspection/dsl.json`` )を生成するかどうか。

    ``true`` ならば生成し、 ``false`` ならば生成しない。

    既定値: ``true``

``inspection.task``
    タスクの構造を可視化するためのファイル( ``etc/inspection/task.json`` )を生成するかどうか。

    ``true`` ならば生成し、 ``false`` ならば生成しない。

    既定値: ``true``

``directio.input.filter.enabled``
    Direct I/O input filterを有効にするかどうか。

    ``true`` ならば有効にし、 ``false`` ならば無効にする。

    既定値: ``true``

``operator.checkpoint.remove``
    DSLで指定した ``@Checkpoint`` 演算子をすべて除去するかどうか。

    ``true`` ならば除去し、 ``false`` ならば除去しない。

    既定値: ``true``

``operator.logging.level``
    DSLで指定した ``@Logging`` 演算子のうち、どのレベル以上を表示するか。

    ``debug`` , ``info`` , ``warn`` , ``error`` のいずれかを指定する。

    既定値: ``info``

``operator.aggregation.default``
    DSLで指定した ``@Summarize`` , ``@Fold`` 演算子の ``partialAggregate`` に ``PartialAggregation.DEFAULT`` が指定された場合に、どのように集約を行うか。

    ``total`` であれば部分集約を許さず、 ``partial`` であれば部分集約を行う。

    既定値: ``total``

``input.estimator.tiny``
    インポーター記述の ``getDataSize()`` に ``DataSize.TINY`` が指定された際、それを何バイトのデータとして見積もるか。

    値にはバイト数か、 ``+Inf`` (無限大)、 ``NaN`` (不明) のいずれかを指定する。

    主に、 ``@MasterJoin`` 系の演算子でJOINのアルゴリズムを決める際など、データサイズによる最適化の情報として利用される。

    既定値: ``10485760`` (10MB)

``input.estimator.small``
    インポーター記述の ``getDataSize()`` に ``DataSize.SMALL`` が指定された際、それを何バイトのデータとして見積もるか。

    その他については ``input.estimator.tiny`` と同様。

    既定値: ``209715200`` (200MB)

``input.estimator.large``
    インポーター記述の ``getDataSize()`` に ``DataSize.LARGE`` が指定された際、それを何バイトのデータとして見積もるか。

    その他については ``input.estimator.tiny`` と同様。

    既定値: ``+Inf`` (無限大)

``operator.join.broadcast.limit``
    ``@MasterJoin`` 系の演算子で、broadcast joinアルゴリズムを利用して結合を行うための、マスタ側の最大入力データサイズ。

    基本的には ``input.estimator.tiny`` で指定した値の2倍程度にしておくのがよい。

    既定値: ``20971520`` (20MB)

``operator.estimator.<演算子注釈名>``
    指定した演算子の入力に対する出力データサイズの割合。

    「演算子注釈名」には演算子注釈の単純名 ( ``Extract`` , ``Fold`` など) を指定し、値には割合 ( ``1.0`` , ``2.5`` など) を指定する。

    たとえば、「 ``operator.estimator.CoGroup`` 」に ``5.0`` を指定した場合、すべての ``@CoGroup`` 演算子の出力データサイズは、入力データサイズの合計の5倍として見積もられる。

    既定値: `operator.estimator.* のデフォルト値`_ を参照

``<バッチID>.<オプション名>``
    指定のオプションを、指定のIDのバッチに対してのみ有効にする。

    バッチIDは ``spark.`` などのプレフィックスが付与する **まえの** ものを指定する必要がある。

    既定値: N/A

``spark.input.direct``
    ジョブフローの入力データを（可能ならば）Sparkから直接読むかどうか。

    これが有効である場合、Direct I/Oではprologueフェーズを省略してSparkから直接ファイルを読み出す。

    WindGateの場合はどちらもSparkからは読み出さず、WindGateのプログラムを利用してファイルシステム上に展開する。

    既定値: ``true``

``spark.parallelism.limit.tiny``
    Sparkでシャッフル処理を行う際に、データサイズの合計が指定のバイト数以下であれば分割数を1に制限する。

    データサイズにはバイト数か、 ``+Inf`` (無限大)、 ``NaN`` (無効化) のいずれかを指定する。

    データサイズは、 ``input.estimator.tiny`` などで指定した見積もりを利用する。

    既定値: ``20971520`` (20MB)

``spark.parallelism.limit.small``
    Sparkでシャッフル処理を行う際に、データサイズの合計が指定のバイト数以下であれば分割数を規定の ``0.5`` 倍に設定する。

    その他については ``spark.parallelism.limit.tiny`` と同様。

    既定値: ``NaN`` (無効化)

``spark.parallelism.limit.regular``
    Sparkでシャッフル処理を行う際に、データサイズの合計が指定のバイト数以下であれば分割数を規定の ``1.0`` 倍に設定する。

    その他については ``spark.parallelism.limit.tiny`` と同様。

    標準では ``+Inf`` が指定されているため、下記の ``large`` や ``huge`` を利用したい場合には有限の値を指定する必要がある。

    既定値: ``+Inf`` (無限大)

``spark.parallelism.limit.large``
    Sparkでシャッフル処理を行う際に、データサイズの合計が指定のバイト数以下であれば分割数を規定の ``2.0`` 倍に設定する。

    その他については ``spark.parallelism.limit.tiny`` と同様。

    既定値: ``+Inf`` (無限大)

``spark.parallelism.limit.huge``
    Sparkでシャッフル処理を行う際に、データサイズの合計が指定のバイト数以下であれば分割数を規定の ``4.0`` 倍に設定する。

    その他については ``spark.parallelism.limit.tiny`` と同様。

    通常の場合、この設定がもっとも大きなデータサイズを表すため、 ``+Inf`` から変更しない方がよい。

    既定値: ``+Inf`` (無限大)

``spark.parallelism.operator.<演算子>``
    指定の演算子を含むSparkのステージに対し、入力データサイズを強制的に指定する。

    データサイズは ``tiny`` , ``small`` , ``regular`` , ``large`` , ``huge`` のいずれかから指定し、それぞれシャッフル時の分割数が ``1`` , ``0.5`` 倍, ``1.0`` 倍, ``2.0`` 倍, ``4.0`` 倍に設定される。

    同一のステージに対して複数の演算子のデータサイズが指定された場合、そのうちもっとも大きなものが利用される。

    既定値: N/A

``spark.planning.option.unifySubplanIo``
    Sparkの等価なステージの入出力を一つにまとめる最適化を有効にするかどうか。

    ``true`` ならば有効にし、 ``false`` ならば無効にする。

    無効化した場合、ステージの入出力データが増大する場合があるため、特別な理由がなければ有効にするのがよい。

    既定値: ``true``

``spark.planning.option.checkpointAfterExternalInputs``
    ジョブフローの入力の直後にチェックポイント処理を行うかどうか。

    ``true`` ならばチェックポイント処理を行い、 ``false`` ならば行わない。

    チェックポイント処理を行う場合、入力データの保存が余計に行われるため、特別な理由がなければ無効にするのがよい。

    なお、Direct I/Oのオリジナルデータを2回以上読みたくない場合にチェックポイント処理が有効な場合があるが、その場合には ``spark.input.direct`` を無効にした方が多くの場合で効率がよい。

    既定値: ``false``

operator.estimator.* のデフォルト値
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

..  list-table:: operator.estimator.* のデフォルト値
    :widths: 3 7
    :header-rows: 1

    * - 演算子注釈名
      - 計算式
    * - ``Checkpoint``
      - 入力の ``1.0`` 倍
    * - ``Logging``
      - 入力の ``1.0`` 倍
    * - ``Branch``
      - 入力の ``1.0`` 倍
    * - ``Project``
      - 入力の ``1.0`` 倍
    * - ``Extend``
      - 入力の ``1.25`` 倍
    * - ``Restructure``
      - 入力の ``1.25`` 倍
    * - ``Split``
      - 入力の ``1.0`` 倍
    * - ``Update``
      - 入力の ``2.0`` 倍
    * - ``Convert``
      - 入力の ``2.0`` 倍
    * - ``Summarize``
      - 入力の ``1.0`` 倍
    * - ``Fold``
      - 入力の ``1.0`` 倍
    * - ``MasterJoin``
      - トランザクション入力の ``2.0`` 倍
    * - ``MasterJoinUpdate``
      - トランザクション入力の ``2.0`` 倍
    * - ``MasterCheck``
      - トランザクション入力の ``1.0`` 倍
    * - ``MasterBranch``
      - トランザクション入力の ``1.0`` 倍
    * - ``Extract``
      - 既定値無し
    * - ``GroupSort``
      - 既定値無し
    * - ``CoGroup``
      - 既定値無し
    
既定値がない演算子に対しては、有効なデータサイズの見積もりを行いません。

制限事項
========

ここでは、Asakusa on Spark固有の制限事項について説明します。これらの制限は将来のバージョンで緩和される可能性があります。

非対応機能
----------

Asakusa on Sparkは、Asakusa Frameworkが提供する以下の機能には対応していません。

* ThunderGate
* レガシーモジュール
* その他該当バージョンで非推奨となっている機能

互換性について
==============

ここではAsakusa on Sparkを利用する場合に考慮すべき、Asakusa Frameworkやバッチアプリケーションの互換性について説明します。

演算子の互換性
--------------

Asakusa on Sparkでは、バッチアプリケーション内の演算子内に定義したstaticフィールドを複数のスレッドから利用する場合があります。このため、演算子クラス内でフィールドにstaticを付与している場合、staticの指定を除去するかフィールド参照がスレッドセーフになるようにしてください。


==============
リリースノート
==============

Release 0.3.1
=============

(開発中)

`Asakusa on Spark 0.3.1 documentation`_

..  _`Asakusa on Spark 0.3.1 documentation`: http://docs.asakusafw.com/asakusa-on-spark/0.3.1/release/ja/html/index.html

新機能と主な変更点
------------------

* Spark バージョン 1.6.2 に対応しました。
* その他、細かな機能改善およびバグフィックスが含まれます。

すべての変更点は :doc:`changelogs` を参照してください。

互換性に関して
--------------

* Asakusa on Spark バージョン 0.3.1 は、 Asakusa Framework バージョン 0.8.1 以降が必要です。

Release 0.3.0
=============

Apr 08, 2016

`Asakusa on Spark 0.3.0 documentation`_

..  _`Asakusa on Spark 0.3.0 documentation`: http://docs.asakusafw.com/asakusa-on-spark/0.3.0/release/ja/html/index.html

新機能と主な変更点
------------------

正式機能として公開
~~~~~~~~~~~~~~~~~~

2015年からDeveloper Previewとして公開していた Asakusa on Spark を、バージョン 0.3.0 より正式機能として公開しました。

Asakusa on Spark Iterative Extentions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Asakusa on Spark の拡張機能「Iterative Extensions」を試験的機能として公開しました。

Iterative Extensionsは、あるバッチに対してバッチ引数の一部または全部を変えながら同じバッチを連続して実行するための機能です。

Iterative Extensionsを適用したバッチを「反復バッチ」と呼びます。
反復バッチは通常のバッチを連続して実行する場合と比べて、次の点で高速に実行できる可能性があります。

* 連続処理によるリソースの効率的な利用

 連続するバッチアプリケーションを1つのSparkアプリケーションとして実行するため、特にYARN上での実行においては、アプリケーションコンテナの初期化などの分散オーバーヘッドが極小化される、コンテナリソースをシンプルな設定で最大限に利用できる、などの利点があります。

* 差分処理による最適化

 反復バッチでは連続するバッチ間で再計算が不要な箇所は実行結果を再利用することがあるため、特に実行するバッチアプリケーション間での変更箇所が少ない場合には、バッチ間の差分処理による利点が大きくなります。

反復バッチは、日付範囲を指定した日次バッチの一括実行や、パラメータ・スイープによるシミュレーションといった用途に適しています。

Iterative Extensionsは、反復バッチを定義するためのAsakusa DSLの拡張構文、反復バッチを生成するするためのAsakusa DSLコンパイラの拡張、および反復バッチを実行するためのインターフェースや実行モジュールなどを提供します。

Asakusa on Spark Iterative Extensions の詳細は、以下のドキュメントを参照してください。

* :doc:`iterative-extension`

その他の変更点
--------------

* Spark バージョン 1.6.1 に対応しました。
* その他、細かな機能改善およびバグフィックスが含まれます。

すべての変更点は :doc:`changelogs` を参照してください。

互換性に関して
--------------

* Asakusa on Spark バージョン 0.3.0 は、 Asakusa Framework バージョン 0.8.0 以降が必要です。

* Asakusa on Spark バージョン 0.3.0 は、 Spark バージョン 1.6.0 以降が必要です。

..  note::
    対応プラットフォームの変更については、 :doc:`user-guide` - :ref:`target-platform` を参照してください。

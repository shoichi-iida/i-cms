# i-cms

`i-cms` は、Python と Tornado をベースにした、WebベースのCGIゲームを開発するための軽量フレームワークです。
XMLなどの外部定義ファイルを用いて動的にページや機能を構築できるのが特徴です。

## 主な機能
- **動的ルーティング**: `functions/page` や `functions/socket` 配下のクラスを自動で読み込み、エンドポイントを生成します。
- **外部定義管理**: TinyDB を使用して XML 形式の定義ファイルをメモリ上で管理し、柔軟なコンテンツ設定が可能です。
- **WebSocket サポート**: リアルタイムな通信を必要とするゲーム機能に対応しています。
- **高機能サーバー設定**: SSHトンネル、SSL/TLS、マルチプロセス起動など、実運用を考慮した機能を標準搭載しています。

## 開発環境
- Python 3.12
- tornado 6.5.4
- tinydb 4.8.2
- requests 2.32.5 
- beautifulsoup4 4.14.3
- lxml 6.0.2
- pymysql 1.1.2
- openpyxl 3.1.5
- paramiko 4.0.0
- python-dateutil 2.9.0
- sshtunnel 0.4.0

## ディレクトリ構成
- `db`: データベース関連ファイル
- `define`: XML等の定義ファイル格納
- `functions`: アプリケーションロジック（ハンドラー、ページ、ソケット定義等）
- `setup`: 初期設定・セットアップ用スクリプト
- `ssl`: SSL証明書関連
- `static`: 静的ファイル（CSS, JS, 画像, フォント等）
- `templates`: HTMLテンプレート
- `tests`: ユニットテスト

## 使い方
### サーバーの起動
Windows環境の場合:
```bash
start_server.bat
```

Linux/Mac環境の場合:
```bash
./start_server.sh
```

## 使用例(サンプル)
https://doll-trpg.net/

お試し用ID : test / test

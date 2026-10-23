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

### 既存DBの移行（アップデート時）
テーブル定義（`define/tables`）の変更前に作成したDBを使い続ける場合は、**新しいバージョンでサーバーを起動する前に**移行スクリプトを実行してください。
パスワードの保存方式がXOR暗号からbcryptへ変わったため、移行しないと既存のアカウントでログインできません。

1. サーバーを停止する
2. `server.ini` の `encrypt_key` が、これまで使用していた値のままであることを確認する（XOR暗号の復号に使用します）
3. リポジトリのルートで移行スクリプトを実行する
   ```bash
   python setup/migrate_data.py
   ```
4. サーバーを起動する

移行スクリプトは次を行います。何度実行しても移行済みのデータは変更しません。
- 定義に追加された列の追加（SQLite・MySQL）。MySQLで `tbl_account.password` が短い文字列型（旧 `setup.sql` の `VARCHAR(30)`）の場合は拡張
- 旧 `setup.sql` で作成した `mst_auth` の列名の変更（`auth_id` → `id`、`auth_name` → `name`。MySQL 8.0以降・MariaDB 10.5.2以降が必要）
- 旧 `setup.sql` で作成した `tbl_account_token` の作り直し（JWT・有効期限の時刻を保持できない型のため。**既存のログインセッションは破棄され、利用者は再ログインが必要です**）
- `tbl_auth` の権限キーの変換（`auth_id`（権限定義のID）→ `function`（`mst_auth.function`））
- `tbl_account` の追加列の補完（`is_password_set`・`login_failed_count`・作成/更新日時等）
- パスワードの移行（XOR暗号 → bcrypt）

変更する前に、SQLite・MySQLでは `{テーブル名}_bak_{日時}` テーブル、TinyDBでは `{DBファイル}.bak_{日時}` ファイルへバックアップを取ります。
移行結果を確認したら、不要になったバックアップは削除してください（XOR暗号のパスワードが残っているため）。

## 使用例(サンプル)
https://doll-trpg.net/

お試し用ID : test / test

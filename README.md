# jetstream-http-push

NATS JetStream から購読側の HTTP の受け口へ push する中継

購読ごとに durable consumer を持ち、メッセージを 1 件ずつ受け口へ `POST` する  
購読に届くのは、その consumer を初めて作った後に publish されたメッセージだけで、中継を再起動すると続きから届く  
リクエストは Cloud Pub/Sub の push subscription で payload unwrapping と write metadata を有効にしたときと同じ形にする

- 本文は publish されたペイロードのバイト列で、`Content-Type` は `application/octet-stream`
- `x-goog-pubsub-message-id` は Watermill のメッセージ UUID。UUID がなければストリームのシーケンス番号
- `x-goog-pubsub-ordering-key` はメタデータ `event_id`。空なら付けない
- `x-goog-pubsub-publish-time` は JetStream に publish された時刻（UTC、RFC 3339）

受け口が 2xx を返せば ack、それ以外の応答や接続の失敗、10 秒のタイムアウトなら `NAK_DELAY` だけ待たせて nak する  
consumer の未 ack は 1 件に限る（`MaxAckPending=1`）ので、同じトピックの次のメッセージは先のメッセージが ack されるまで届かない  
再送に上限はなく、デッドレターも置かない

## 開発

### セットアップ

mise を使用して開発環境をセットアップする  
`mise install` の後処理で lefthook の pre-commit フック（シークレット検出と golangci-lint）が導入される

```bash
mise trust
mise install
```

### テストと lint

```bash
go build ./...
go test ./...
golangci-lint run
```

### e2e テスト

```bash
task e2e
```

Docker Compose で NATS、中継、e2e テストのコンテナを起動し、e2e テストの終了コードで終わる。終了後に `docker compose --profile e2e down -v` で片付ける  
e2e テストのコンテナは受け口を `:8080` で待ち受け、Watermill の JetStream publisher で publish して、届いたリクエストを確かめる

### 環境変数

| 環境変数 | デフォルト | 内容 |
| --- | --- | --- |
| `NATS_URL` | なし（必須） | NATS の接続先 |
| `SUBSCRIPTIONS` | なし（必須） | 購読の JSON 配列。要素は `{"name": "...", "topic": "...", "url": "..."}` で、`name` は durable consumer の名前、`topic` はストリームと subject の名前、`url` は受け口の http または https の絶対 URL。`name` の重複、JetStream の名前に使えない文字（`.`、`*`、`>`、空白、`/`、`\`）、未知のフィールドがあると起動しない |
| `NAK_DELAY` | `5s` | 受け口が失敗したメッセージを再送するまでの待ち時間。`time.ParseDuration` が解釈する形式の正の値 |
| `SERVER_ADDR` | `:8080` | `GET /healthz` と `GET /readyz` を待ち受けるアドレス。`/readyz` はすべての購読が始まり、NATS への接続がすべてつながっているときだけ 200 を返す |
| `LOG_LEVEL` | `info` | ログに出力する最小レベル。`debug`／`info`／`warn`（`warning` も同義）／`error`／`critical` を取り、未知の値なら起動しない |

ストリームがなければ `topic` の名前で作り、あれば設定を変えずに使う  
ログは標準出力へ 1 行 1 件の JSON で書き出す

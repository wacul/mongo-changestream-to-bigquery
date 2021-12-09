# mongo-changestream-to-bigquery

mongoのchange stream apiを使って差分をbigqueryに取り込むスクリプトです。
常に起動せずに、時間ごとに起動して差分を取り込むためのものです。

* bigqueryでリアルタイムに差分を更新するような方法は高くつく
* エラーが出た時の処理が面倒

などが理由です。

動作の流れは下記です。

1. bigqueryにある最新のtimestamp(time,increment)を取得する
3. mongoのchange streamをtimeとincrementの地点から取得する
   1. 内容はfulldocumentに記載されているもの
      1. timestampとしてbsonに入っているtimeとincrementも含める
      2. _idは$oidを抜く
   2. insertはinsert用の一時ファイルに
   3. update or upsert
      1. input modeがmergeならupdate用の一時ファイル
      2. input modeがappendならinsert用の一時ファイル
   4. deleteならdelete用のidのみ一時ファイルに
   5. もしDELETEだけが実行された上に、最新のtime,incrementに対しての削除行われたなら、timeとincrementだけ最新のものを入れる。latest viewにはこれは表示されない。
   6. もし、DELETE後に同じ_idがINSERTされていたら、削除対象から外してinput modeに応じて insert一時ファイル or update一時ファイルに書き込む
4. timeとincrementの最新断面が見れるlatest viewがなければ作成する
5. bigqueryに対してINSERT or MERGE or DELETEする

## install

```
pip3 install git+ssh://git@github.com/wacul/inhouse-data/mongo-changestream-to-bigquery/
# or
pip3 install git+https://github.com/wacul/inhouse-data/mongo-changestream-to-bigquery/
```

## usage

下記を実行するだけです。

```:bash
mongo-changestream-to-bigquery --config config.yml
```

認証はgcpのSDKに任せるので、キーを使う場合は環境変数 `GOOGLE_APPLICATION_CREDENTIALS=` などを使ってください。



### 初期の取り込み方法

replicaを使ってsnapshot時点のtimestampを使って初期の時間合わせをします。

1. replicationを止めたりsnapshotなどからmongoを起動する
2. oplogの最新のtimestampを下記コマンドで取得しておく

```:bash
use local
db.oplog.rs.find().sort({$natural: -1}).limit(1)
```

3. mongoのデータをexport

```:bash
mongoexport -d database -c collection -o collection.json
```

4. bigqueryにアップロード

2で取得したtimeとincrementを入れてexportしたデータをinsertします。
テーブルはここで管理することを想定してないので、作成されていることを前提にしてます。

```:bash
mongo-changestream-to-bigquery --config config.yml mongoexport-insert -e collection.json -t timestamp -i increment
```

## config.yml.example

liquidが使えるので `{{ env.MONGO_URI }}` のような指定ができます。パスワードなどsecretな情報はこれで隠してください。

### input_mode

#### `append`

項目のアップデートであっても必ずinsertをします。
最新の断面はtimeとincrementが最新であるviewを作って参照することになります。
過去の変更履歴を見ることができます。

##### `merge`

項目のアップデートがあった場合にはbigqueryのMERGEクエリを使って項目をアップデートします。
過去の履歴は消えます

### time_field, increment_field

最新断面を得るためと、どこまでmongoからデータを取得したかを保持するためのフィールドです。
mongoのフィールドに存在しないフィールド名を指定してください。
既存のフィールド名を入れてしまったら、そのフィールドはtime_fieldまたはincrement_fieldとして扱われてデータは上書きされます。

## mongodb

### uri

mongodbへの接続のuriです

### db, collection

change streamを取得するmongodbのdb, collection 名を指定してください。

## bigquery

### project, dataset, table

bigqueryのテーブルの場所を指定してください。

### schema_file

bigqueryのschemaのjsonファイルをそのまま指定してください。
指定されているフィールドのみアップロード対象にします。

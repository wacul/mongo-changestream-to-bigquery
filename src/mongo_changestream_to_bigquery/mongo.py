import pymongo
import logging
import json
from bson import Timestamp
from bson.json_util import dumps
from .change_stream_to_bigquery import BigquerySchema
from .utils import InputMode


logger = logging.getLogger(__name__)


class Mongo(object):
    def __init__(
        self, mongo_uri: str, db: str, collection: str,
        time_field: str, increment_field: str, input_mode: InputMode
    ) -> None:
        super().__init__()
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = db
        self.collection = collection
        self.time_field = time_field
        self.increment_field = increment_field
        self.input_mode = input_mode

    def write_change_stream(
        self, start_at_operation_time: Timestamp,
        insert_file_path: str, delete_file_path: str, update_file_path: str,
        bigquery_schema: BigquerySchema,
    ):
        # 特定テーブルだけ対象にする
        pipeline = [
            {"$match": {"operationType": {"$in": ["insert", "delete", "replace", "update"]}}},
            {"$match": {"ns.db": self.db}},
            {"$match": {"ns.coll": self.collection}},
        ]
        change_stream = self.client.watch(
            full_document="updateLookup",
            start_at_operation_time=start_at_operation_time,
            pipeline=pipeline,
        )

        insert_file = open(insert_file_path, "w")
        update_file = open(update_file_path, "w")
        delete_file = open(delete_file_path, "w")
        delete_ids = []
        change_stream_latest_time = None
        change_stream_latest_increment = None

        with change_stream as stream:
            # change streamが一時なくなるまで読み取る
            while stream.alive:
                change = stream.try_next()
                if change is None:
                    break

                d = json.loads(dumps(change))
                logger.debug(d)
                time = d["clusterTime"]["$timestamp"]["t"]
                increment = d["clusterTime"]["$timestamp"]["i"]
                change_stream_latest_time = time
                change_stream_latest_increment = increment

                if d["operationType"] == "delete":
                    BigquerySchema.remove_bson_key(d["documentKey"])
                    delete_ids.append(d["documentKey"]["_id"])
                    continue

                document = bigquery_schema.filter_document(d["fullDocument"])

                # timeとincrementをつける
                document.update({self.time_field: time})
                document.update({self.increment_field: increment})

                if d["operationType"] == "insert":
                    # 削除後に同じ_idをいれていたら、削除対象からはずす
                    try:
                        delete_ids.remove(document["_id"])
                        if self.input_mode == InputMode.merge:
                            update_file.write(json.dumps(document) + "\n")
                            continue
                        if self.input_mode == InputMode.append:
                            insert_file.write(json.dumps(document) + "\n")
                            continue
                        raise Exception("no input mode")
                    except ValueError:
                        # 削除対象に入ってなかったら普通にinsert
                        insert_file.write(json.dumps(document) + "\n")
                        continue
                if d["operationType"] == "update" or d["operationType"] == "replace":
                    # mergeならupdateに入れてMERGE対象
                    if self.input_mode == InputMode.merge:
                        update_file.write(json.dumps(document) + "\n")
                        continue
                    # appendならinsertにいれて追記
                    if self.input_mode == InputMode.append:
                        insert_file.write(json.dumps(document) + "\n")
                        continue
                    raise Exception("no input mode")
                raise Exception("no operation type")
        for id in delete_ids:
            delete_file.write(id + "\n")

        return (change_stream_latest_time, change_stream_latest_increment)

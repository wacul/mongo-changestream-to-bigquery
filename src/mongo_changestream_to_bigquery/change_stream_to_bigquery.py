from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from bson import Timestamp
import logging
import typing
import json
import os
import io
from functools import reduce
from .utils import InputMode


logger = logging.getLogger(__name__)


class BigquerySchema(object):
    def __init__(self, bigquery_schema_file: io.BufferedReader) -> None:
        super().__init__()
        self.schema = json.load(bigquery_schema_file)
        self.schema_keys = self.recursive_dict_key(self.schema)

    @staticmethod
    def deep_get(dictionary, keys, default=None) -> dict:
        value = reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), dictionary)
        d = {}
        if value is not None:
            BigquerySchema.deep_put(d, keys, value)
        return d

    @staticmethod
    def deep_put(dictionary, keys, item):
        if "." in keys:
            key, rest = keys.split(".", 1)
            if key not in dictionary:
                dictionary[key] = {}
            BigquerySchema.deep_put(dictionary[key], rest, item)
        else:
            dictionary[keys] = item

    @staticmethod
    def recursive_dict_key(bigquery_schema: dict, field_name: str = '') -> list:
        schema_keys = []
        for field in bigquery_schema:
            key = f"{field_name}{'.' if field_name else ''}{field['name']}"
            if field["type"] == "RECORD":
                record_fields = field["fields"]
                schema_keys.extend(BigquerySchema.recursive_dict_key(record_fields, key))
            else:
                schema_keys.append(key)
        return schema_keys

    @staticmethod
    def remove_bson_key(document: dict):
        for k, v in document.items():
            if type(v) == dict:
                r = list(document[k].keys())[0]
                if r[0] == '$':
                    document[k] = document[k][r]

    def filter_document(self, document: dict):
        filtered_doc = {}
        BigquerySchema.remove_bson_key(document)
        for k in self.schema_keys:
            filtered_doc.update(self.deep_get(document, k))
        return filtered_doc


class ChangeStreamToBigquery(object):
    def __init__(
        self, project: str, dataset: str, table: str,
        time_field: str, increment_field: str,
    ) -> None:
        super().__init__()
        self.client = bigquery.Client()
        self.table_id = '.'.join([
            project,
            dataset,
            table,
        ])
        self.tmp_table_id = self.table_id + "_tmp"
        self.latest_view_table_id = self.table_id + "_latest"
        self.time_field = time_field
        self.increment_field = increment_field
        time, increment, id = self._bigquery_latest_timestamp()
        # start_at_operation_time は指定から取得するので、incrementを+1しておく
        self.start_at_operation_time = Timestamp(time, increment + 1)
        self.bigquery_latest_time = time
        self.biguqery_latest_increment = increment
        self.bigquery_latest_id = id

    def run(
        self, insert_file_path: str, delete_file_path: str, update_file_path: str,
        change_stream_latest_time: int, change_stream_latest_increment: int,
        bigquery_schema: BigquerySchema, input_mode: InputMode
    ):
        self._create_latest_view()
        insert_file_size = 0
        with open(insert_file_path, "rb") as file:
            insert_file_size = os.stat(insert_file_path).st_size
            if insert_file_size > 0:
                self.insert_row_bigquery(file=file)
            else:
                logger.info("no insert rows")
        update_file_size = 0
        if input_mode == InputMode.merge:
            with open(update_file_path, "rb") as file:
                update_file_size = os.stat(update_file_path).st_size
                if update_file_size > 0:
                    self._merge_row_bigquery(
                        file=file, bigquery_schema=bigquery_schema
                    )
                else:
                    logger.info("no update rows")
        with open(delete_file_path, "r") as file:
            delete_ids = []
            for id in file.readlines():
                delete_ids.append(id.rstrip("\n"))
            if len(delete_ids) > 0:
                # 最新の_idを削除してしまう場合、time と incrementを残す
                if insert_file_size == 0 and update_file_size == 0:
                    if self.bigquery_latest_id in delete_ids:
                        self._insert_timestamp(
                            time=change_stream_latest_time, increment=change_stream_latest_increment
                        )
                self._delete_ids_bigquery(delete_ids=delete_ids)
            else:
                logger.info("no delete rows")

    def insert_row_bigquery(self, file: io.BufferedRWPair):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = self.client.load_table_from_file(file, self.table_id, job_config=job_config)
        try:
            job.result()
            logger.info(
                f"Insert rows to {self.table_id}"
            )
        except Exception as e:
            for e in job.errors:
                logger.error(f"{e['message']}")
            raise

    def _merge_row_bigquery(self, file: io.BufferedRWPair, bigquery_schema: BigquerySchema):
        # MERGEのためにtmp_tableを作る
        tmp_table = bigquery.Table(self.tmp_table_id, bigquery_schema.schema)
        self.client.create_table(table=tmp_table, exists_ok=True)
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            job = self.client.load_table_from_file(file, self.tmp_table_id, job_config=job_config)
            try:
                job.result()
            except Exception as e:
                for e in job.errors:
                    logger.error(f"{e['message']}")
                raise
            # tmp_tableにあるデータで_idが合致するものをUPDATEする
            # updateのみが対象なので、合致するものがないのはおかしいのでエラーにしてしまう
            # エラーになったらデータを入れ直すべき
            query_job = self.client.query(
                f"""MERGE `{self.table_id}` target USING
                (SELECT * FROM `{self.tmp_table_id}` ORDER BY {self.time_field} DESC, {self.increment_field} DESC LIMIT 1) tmp
                ON target._id = tmp._id
                WHEN MATCHED THEN
                    UPDATE SET {','.join(f" {s} = tmp.{s}" for s in bigquery_schema.schema_keys)}
                WHEN NOT MATCHED THEN
                    INSERT (_id) VALUES ( ERROR('ERROR: MERGE taget not found.') )"""
            )
            try:
                query_job.result()
                logger.info(
                    f"Update rows to {self.table_id}"
                )
            except Exception as e:
                for e in query_job.errors:
                    logger.error(f"{e['message']}")
                raise
        finally:
            self.client.delete_table(self.tmp_table_id, not_found_ok=True)

    def _insert_timestamp(self, time: int, increment: int):
        logger.info(f"""Delete latest timestamp _id: {self.bigquery_latest_id}.
Insert time and increment field.""")
        d = {self.time_field: time, self.increment_field: increment}
        job = self.client.load_table_from_json(
            [d],
            self.table_id
        )
        try:
            job.result()
        except Exception as e:
            for e in job.errors:
                logger.error(f"{e['message']}")
            raise

    def _delete_ids_bigquery(self, delete_ids: typing.List):
        delete_ids_in_string = ','.join(f'"{id}"' for id in delete_ids)
        query_job = self.client.query(
            f"""
            DELETE
            FROM `{self.table_id}`
            WHERE _id in ({delete_ids_in_string})"""
        )
        try:
            query_job.result()
            logger.info(f"bigquery `{self.table_id}` delete ids {delete_ids}")
        except Exception as e:
            for e in query_job.errors:
                logger.error(f"{e['message']}")
            raise

    def _create_latest_view(self):
        try:
            self.client.get_table(self.latest_view_table_id)
        except NotFound:
            view = bigquery.Table(self.latest_view_table_id)
            view.view_query = f"""SELECT
  agg.table.*
FROM (
  SELECT
    _id,
    ARRAY_AGG(STRUCT(table)
    ORDER BY
      {self.time_field} DESC, {self.increment_field} DESC)[SAFE_OFFSET(0)] agg
  FROM
    `{self.table_id}` table
WHERE _id is not null
GROUP BY _id)"""
            self.client.create_table(view)
            logger.info(f"create latest view `{self.latest_view_table_id}`")

    def _bigquery_latest_timestamp(self) -> typing.Tuple:
        query_job = self.client.query(
            f"""
            SELECT
                _id, {self.time_field}, {self.increment_field}
            FROM `{self.table_id}`
            ORDER BY {self.time_field} DESC, {self.increment_field} DESC
            LIMIT 1"""
        )
        result = query_job.result().next()
        time = result[self.time_field]
        increment = result[self.increment_field]
        id = result["_id"]
        if time is None or increment is None:
            raise Exception(f"{self.time_field}または{self.increment_field}が取得できません")
        logger.info(f"bigquery `{self.table_id}` latest time: {time}, increment: {increment}")

        return (time, increment, id)

import os
import sys
import logging
import argparse
import yaml
from liquid import Environment
from .change_stream_to_bigquery import ChangeStreamToBigquery, BigquerySchema
from .mongo import Mongo
from .utils import InputMode
import tempfile
import json
import io


logger = logging.getLogger(__name__)


# Arguments parsing
def init():
    parser = argparse.ArgumentParser(description='Mongo Change Stream to Bigquery')
    parser.add_argument('--config', type=argparse.FileType('r'), required=True)
    subparser = parser.add_subparsers(dest='command')
    export_parse_parser = subparser.add_parser("mongoexport-insert", help="mongoexport file to bigquery")
    export_parse_parser.add_argument('--export-file', '-e', type=argparse.FileType('r'), required=True)
    export_parse_parser.add_argument('--time', '-t', type=int, required=True)
    export_parse_parser.add_argument('--increment', '-i', type=int, required=True)

    argp = parser.parse_args()
    return argp


def mongoexport_insert(
    bq: ChangeStreamToBigquery, bigquery_schema: BigquerySchema, mongoexport_file: io.BufferedReader,
    time_field: str, increment_field: str, time: int, increment: int
):
    mongoexport = json.load(mongoexport_file)
    with tempfile.TemporaryDirectory() as tempdir:
        insert_file_path = os.path.join(tempdir, "insert")
        insert_file = open(insert_file_path, "w")
        for e in mongoexport:
            document = bigquery_schema.filter_document(e)
            document.update({time_field: time})
            document.update({increment_field: increment})
            logger.debug(document)
            insert_file.write(json.dumps(document) + "\n")
        insert_file.close()
        file = open(insert_file_path, "rb")
        bq.insert_row_bigquery(file=file)


def load_change_stream(mongo: Mongo, bq: ChangeStreamToBigquery, bigquery_schema: BigquerySchema, input_mode: InputMode):
    with tempfile.TemporaryDirectory() as tempdir:
        insert_file_path = os.path.join(tempdir, "insert")
        update_file_path = os.path.join(tempdir, "update")
        delete_file_path = os.path.join(tempdir, "delete")

        change_stream_latest_time, change_stream_latest_increment = mongo.write_change_stream(
            start_at_operation_time=bq.start_at_operation_time,
            insert_file_path=insert_file_path,
            update_file_path=update_file_path,
            delete_file_path=delete_file_path,
            bigquery_schema=bigquery_schema,
        )

        bq.run(
            insert_file_path=insert_file_path,
            update_file_path=update_file_path,
            delete_file_path=delete_file_path,
            change_stream_latest_time=change_stream_latest_time,
            change_stream_latest_increment=change_stream_latest_increment,
            bigquery_schema=bigquery_schema,
            input_mode=input_mode,
        )


def main():
    log_level = logging.INFO
    if bool(os.getenv("DEBUG")):
        log_level = logging.DEBUG

    logging.basicConfig(stream=sys.stdout, level=log_level, format='%(levelname)s: %(message)s')
    logging.getLogger("botocore").setLevel(logging.WARNING)

    args = init()
    renderd_config = Environment().from_string(args.config.read()).render(env=os.environ)
    config = yaml.safe_load(renderd_config)
    with open(config["bigquery"]["schema_file"], "r") as file:
        bs = BigquerySchema(file)
    bq = ChangeStreamToBigquery(
        project=config["bigquery"]["project"],
        dataset=config["bigquery"]["dataset"],
        table=config["bigquery"]["table"],
        time_field=config["timeField"],
        increment_field=config["incrementField"],
    )
    mongo = Mongo(
        mongo_uri=config["mongodb"]["uri"],
        db=config["mongodb"]["db"],
        collection=config["mongodb"]["collection"],
        time_field=config["timeField"],
        increment_field=config["incrementField"],
        input_mode=InputMode(config["inputMode"]),
    )

    if args.command == 'mongoexport-insert':
        mongoexport_insert(
            bq=bq, bigquery_schema=bs, mongoexport_file=args.export_file,
            time_field=config["timeField"], increment_field=config["incrementField"],
            time=args.time, increment=args.increment
        )
    else:
        load_change_stream(
            mongo=mongo, bq=bq, bigquery_schema=bs, input_mode=InputMode(config['inputMode'])
        )


if __name__ == '__main__':
    main()

import json
import boto3
import gzip
from datetime import datetime, timedelta, timezone

"""This script would convert the conventional json into ndjson json format
Since athena does not accept json format we have to go through either ETL job to convert json or we can use this script
to flatten the json file so that i can be crawled and presented in athena"""


def lambda_handler(event, context):
    s3_client = boto3.client("s3")

    bucket = event['Records'][0]['s3']['bucket']['name']

    key = event['Records'][0]['s3']['object']['key']

    def get_millisecond_unix_time(timestamp):
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        return (timestamp - epoch) // timedelta(milliseconds=1)

    now = datetime.now(timezone.utc)
    ms_unix_time = get_millisecond_unix_time(now)
    output_key = "{}/{}/{}--{}.json".format("flatten-file/flatten-feed-file", now.strftime("%Y/%m/%d"), "flatten-file",
                                            ms_unix_time)
    output_key_latest = "{}/{}.json".format("latest/file", "json_file")

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)

        compressed = response['Body'].read()
        decompressed = gzip.decompress(compressed)
        data_json = json.loads(decompressed)

        def flatten_json(obj):
            service = "match"
            if service in obj['attributes']:
                del obj["attributes"][service]
            ret = {}

            def flatten(x, flattened_key=""):

                if type(x) is dict:
                    for current_key in x:
                        if current_key == "collapsed":
                            ret["CIDR"] = x["collapsed"]
                        elif current_key == "tags":
                            ret[f"{current_key[:-1]}"] = x["tags"]
                        elif current_key == "domains":
                            ret[f"{current_key[:-1]}"] = x["domains"]
                        elif current_key == "id" or current_key == "type":
                            pass
                        else:
                            flatten(x[current_key], flattened_key + current_key + "_")
                elif type(x) is list:
                    i = 0
                    for elem in x:
                        flatten(elem, flattened_key + str(i) + "_")
                        i += 1

                else:
                    ret[flattened_key[:-1]] = str(x)

            flatten(obj)

            with open("/tmp/test.json", mode="a") as f:
                f.write(json.dumps(ret))
                f.write("\n")

        for b in data_json["data"]:
            flatten_json(b)


    except Exception as e:
        print(e)
        raise e
    file_name = "/tmp/test.json"
    s3_client.upload_file(file_name, bucket, output_key)
    s3_client.upload_file(file_name, bucket, output_key_latest)

    print('done')

import csv
import gzip
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
import csv_diff
from botocore.exceptions import ClientError

maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt / 10)

CLOUD_ADDRESS_FEED_BUCKET = os.environ['CLOUD_ADDRESS_FEED_BUCKET']


def get_millisecond_unix_time(timestamp):
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (timestamp - epoch) // timedelta(milliseconds=1)


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return response


def get_feed_json():
    """function for getting feed from s3
    this function takes latest file and one before that"""

    d = datetime.now()
    today = d.date()
    yesterday = today - timedelta(days=1)
    fina_date = str(yesterday).split("-")
    s3 = boto3.client("s3")
    bucket_name = CLOUD_ADDRESS_FEED_BUCKET
    paginator = s3.get_paginator('list_objects')
    all_key = [(page['Key']) for page in paginator.paginate(Bucket=bucket_name).search("Contents[?Size > `0`][]")]
    name = []
    for i in all_key:
        if i.startswith(f'{fina_date[0]}/{fina_date[1]}/{fina_date[2]}'):
            if "cloud_addresses--" in i:
                name.append(i)
    firstfile = ""
    if len(name) >= 1:
        firstfile = str(name[0])
    else:
        print(f'No file was found in {fina_date[0]}/{fina_date[1]}/{fina_date[2]} , could not make release note')
        exit()

    print('{}: Getting Latest Feed'.format(datetime.now().isoformat()))
    latest_file = s3.get_object(
        Bucket=CLOUD_ADDRESS_FEED_BUCKET, Key="latest/cloud_addresses.json.gz"
    )

    print(f'Getting previous feed {firstfile}')
    previous_file = s3.get_object(
        Bucket=CLOUD_ADDRESS_FEED_BUCKET, Key=firstfile
    )

    previous_file_1 = gzip.decompress(previous_file["Body"].read())
    latest_file_1 = gzip.decompress(latest_file["Body"].read())
    return {
        "old-file": json.loads(previous_file_1),
        "new-file": json.loads(latest_file_1),
    }


def filter_data():
    print('{}: Final filter process for release note'.format(datetime.now().isoformat()))
    with open("buffer_data/converted-csv.json", "r") as jsonFile:
        date = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
        data = json.load(jsonFile)
        for i in data["changed"]:
            try:
                grab_changes = i["changes"]
                old_hostnames = list(i["changes"]["Hostnames"][0].split(","))
                new_hostnames = list(i["changes"]["Hostnames"][1].split(","))
                b, a = set(old_hostnames), set(new_hostnames)
                grab_changes.update(
                    {"Hostnames": {"added": list(a - b), "removed": list(b - a)}}
                )
                with open(f"buffer_data/Final-json_{date}.json", "w") as f:
                    f.write((json.dumps(data, indent=2, separators=(",", ": "))))

            except:
                pass

            try:
                grab_changes = i["changes"]
                old_tags = list(i["changes"]["Tags"][0].split(","))
                new_tags = list(i["changes"]["Tags"][1].split(","))
                b, a = set(old_tags), set(new_tags)
                grab_changes.update(
                    {"Tags": {"added": list(a - b), "removed": list(b - a)}}
                )
                with open(f"buffer_data/Final-json_{date}.json", "w") as f:
                    f.write((json.dumps(data, indent=2, separators=(",", ": "))))

            except:
                pass

            try:
                grab_changes = i["changes"]
                old_domain = list(i["changes"]["Domains"][0].split(","))
                new_domain = list(i["changes"]["Domains"][1].split(","))
                b, a = set(old_domain), set(new_domain)
                grab_changes.update(
                    {"Domains": {"added": list(a - b), "removed": list(b - a)}}
                )
                with open(f"buffer_data/Final-json_{date}.json", "w") as f:
                    f.write((json.dumps(data, indent=2, separators=(",", ": "))))

            except:
                pass

            try:
                grab_changes = i["changes"]
                old_only_hostname = list(i["changes"]["OnlyHostnames"][0].split(","))
                new_only_hostname = list(i["changes"]["OnlyHostnames"][1].split(","))
                b, a = set(old_only_hostname), set(new_only_hostname)
                grab_changes.update(
                    {"CIDR": {"added": list(a - b), "removed": list(b - a)}}
                )
                with open(f"buffer_data/Final-json_{date}.json", "w") as f:
                    f.write((json.dumps(data, indent=2, separators=(",", ": "))))

            except:
                pass

            try:
                grab_changes = i["changes"]
                old_CIDR = list(i["changes"]["CIDR"][0].split(","))
                new_CIDr = list(i["changes"]["CIDR"][1].split(","))
                b, a = set(old_CIDR), set(new_CIDr)
                grab_changes.update(
                    {"CIDR": {"added": list(a - b), "removed": list(b - a)}}
                )
                with open(f"buffer_data/Final-json_{date}.json", "w") as f:
                    f.write((json.dumps(data, indent=2, separators=(",", ": "))))

            except:
                pass

    now = datetime.now(timezone.utc)
    ms_unix_time = get_millisecond_unix_time(now)
    output_key = "{}/{}/{}--{}.json".format(
        "release_note", now.strftime("%Y/%m/%d"), "release_note", ms_unix_time
    )
    try:
        print('{}: Uploading file'.format(datetime.now().isoformat()))
        upload_file(
            f"buffer_data/Final-json_{date}.json", CLOUD_ADDRESS_FEED_BUCKET, output_key
        )
        os.remove(f"buffer_data/Final-json_{date}.json")
        print("done")
    except:
        pass
        print('File processing failed')


def sorting(item):
    if isinstance(item, dict):
        return sorted((key, sorting(values)) for key, values in item.items())
    if isinstance(item, list):
        return sorted(sorting(x) for x in item)
    else:
        return item


def format_tags(tags):
    return ",".join(tags)


def csv_diff1():
    try:
        print('{}: Comparing CSV Files'.format(datetime.now().isoformat()))
        diff = csv_diff.compare(
            csv_diff.load_csv(open("buffer_data/old.csv"), key="Name"),
            csv_diff.load_csv(open("buffer_data/new.csv"), key="Name"),
        )
        with open("buffer_data/converted-csv.json", "w") as f:
            abc = json.dumps(diff, indent=2)
            f.write(abc)
        os.remove("buffer_data/old.csv")
        os.remove("buffer_data/new.csv")
        print('{}: Buffer CSV Removed'.format(datetime.now().isoformat()))
    except Exception as e:
        print(e)


def convert_csv():
    data1 = get_feed_json()
    feed_dict = data1["old-file"]
    feed_dict1 = data1["new-file"]

    if "data" not in (feed_dict and feed_dict1):
        print("Formatting Error: data not found")
        sys.exit(-1)
    ordered_entries = sorted(feed_dict["data"], key=lambda x: x["id"])
    ordered_entries1 = sorted(feed_dict1["data"], key=lambda x: x["id"])

    with open("buffer_data/old.csv", "w") as oldf, open(
            "buffer_data/new.csv", "w"
    ) as newf:

        fieldname = [
            "Name",
            "Id",
            "Tags",
            "Hostnames",
            "Onlyhostnames",
            "Domains",
            "CIDR",
            "Description",
        ]
        write_outfile = csv.DictWriter(oldf, fieldnames=fieldname, delimiter="\t")
        write_outfile.writeheader()

        write_outfile1 = csv.DictWriter(newf, fieldnames=fieldname, delimiter="\t")
        write_outfile1.writeheader()

        print('{}: Coverting old feed into CSV'.format(datetime.now().isoformat()))
        try:
            for entry in ordered_entries:
                some_dns_data = "domains" in entry["attributes"]
                all_dns_data = "match" not in entry["attributes"]
                if "collapsed" in entry["attributes"].keys():
                    collapsed = entry["attributes"]["collapsed"]
                else:
                    collapsed = ""
                Name = entry["attributes"]["name"]
                Id = entry["id"]
                Tags = format_tags(entry["attributes"]["tags"])
                Hostnames = some_dns_data
                OnlyHostname = all_dns_data
                Domains = format_tags(entry["attributes"]["domains"])
                Ips = format_tags(collapsed)
                Description = entry["attributes"]["description"]

                old = {
                    "Name": Name,
                    "Id": Id,
                    "Tags": Tags,
                    "Hostnames": Hostnames,
                    "Onlyhostnames": OnlyHostname,
                    "Domains": Domains,
                    "CIDR": Ips,
                    "Description": Description,
                }
                write_outfile.writerow(old)
        except Exception as e:
            print(e)

        print('{}: Coverting new feed into CSV'.format(datetime.now().isoformat()))
        try:
            for entry1 in ordered_entries1:
                some_dns_data1 = "domains" in entry1["attributes"]
                all_dns_data1 = "match" not in entry1["attributes"]
                if "collapsed" in entry1["attributes"].keys():
                    collapsed1 = entry1["attributes"]["collapsed"]
                else:
                    collapsed1 = ""
                Name1 = entry1["attributes"]["name"]
                Id1 = entry1["id"]
                Tags1 = format_tags(entry1["attributes"]["tags"])
                Hostnames1 = some_dns_data1
                OnlyHostname1 = all_dns_data1
                Domains1 = format_tags(entry1["attributes"]["domains"])
                Ips1 = format_tags(collapsed1)
                Description1 = entry1["attributes"]["description"]

                new = {
                    "Name": Name1,
                    "Id": Id1,
                    "Tags": Tags1,
                    "Hostnames": Hostnames1,
                    "Onlyhostnames": OnlyHostname1,
                    "Domains": Domains1,
                    "CIDR": Ips1,
                    "Description": Description1,
                }

                write_outfile1.writerow(new)
        except Exception as e:
            print(e)


def final_main():
    directory = f"{os.getcwd()}/buffer_data"
    if not os.path.exists(directory):
        os.mkdir(directory)

    convert_csv()
    csv_diff1()
    filter_data()


if __name__ == "__main__":
    final_main()


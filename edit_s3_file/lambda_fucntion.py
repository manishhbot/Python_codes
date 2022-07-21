""" lambda function to list out the URI of all the json files in a nested s3 ecosystem"""

import boto3

bucket_name = 'bucket_name'
s3 = boto3.resource('s3')


def lambda_handler(event, context):
    my_bucket = s3.Bucket(bucket_name)

    final_list = []

    for obj in my_bucket.objects.all():
        oo = obj.key
        if oo.startswith("json/") and oo.endswith(".json"):
            url = f's3://{bucket_name}/{oo}'
            final_list.append(url)
            print(url)

        else:
            pass
    return final_list

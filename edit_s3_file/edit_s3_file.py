import os
import json
import boto3
import country_converter as coco

BUCKET_NAME = "bucket_name"

'''put paths of the file in FILE_NAME
for example 'json/year=2022/month=05/day=01/hour=00/file.json'
'''

FILE_NAME = [
    'json/year=2022/month=05/day=01/hour=00/file_name.json',
    'json/year=2022/month=05/day=01/hour=23/file_name.json'
]

for i in FILE_NAME:
    upload_path = i
    s3 = boto3.client("s3")
    s3.download_file(Bucket=BUCKET_NAME, Key=i, Filename="file_name_temp.json")

    try:
        with open("file_name_temp.json", "r") as f:
            lines = f.readlines()

            for line in lines:
                file_dict = json.loads(line)
                file_dict.items()
                '''put key of the json dict where you want the change
                for example'''
                date_key = file_dict['abc'][0]['date']

                '''make changes here'''
                final_out = date_key[:16] + date_key[19:]
                file_dict['datasets'][0]['date'] = final_out

                with open('file_name_hold.json', 'a+') as f:
                    json.dump(file_dict, f)
                    f.write('\n')

            os.remove('file_name_temp.json')

            with open('file_name_hold.json', "r") as file:
                raw_data = {}
                lines = file.readlines()
                ab = len(lines)
                for i in range(ab):
                    first_raw = json.loads(lines[i])
                    data = first_raw['properties']
                    for item in data:
                        if item['key'] == 'abc':
                            index_number = data.index(item)
                            aaa = item['value']
                            try:
                                iso2_codes = coco.convert(names=aaa, to='ISO2')
                                if iso2_codes == "not found":
                                    first_raw['properties'][index_number]['value'] = aaa
                                else:
                                    first_raw['properties'][index_number]['value'] = iso2_codes
                            except:
                                pass
                            with open('file_name.json', 'a+') as f:
                                json.dump(first_raw, f)
                                f.write('\n')
                        else:
                            pass
        os.remove('file_name_hold.json')
        try:
            s3.upload_file(
                Filename="file_name.json",
                Bucket=BUCKET_NAME,
                Key=upload_path,
            )
        except Exception as r:
            raise r
        print('upload done')
        os.remove('na_enriched.json')
    except:
        print(i)
        pass

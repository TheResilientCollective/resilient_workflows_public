"""Publish the Tijuana River dashboard as a static page to the Resilient S3 bucket.

Usage: python3 publish_s3.py <out_dir> [--bucket resilentpublic] [--prefix latest/tijuana/dashboard/]

<out_dir> is what build_data.py wrote. The page is page.html wrapped in standalone.html,
which supplies the small data runtime the page expects; data goes under data/<id>.json.
Credentials come from S3_ADDRESS, S3_ACCESS_KEY and S3_SECRET_KEY (needs the minio package).
"""
import argparse
import glob
import io
import json
import os

from minio import Minio

HERE = os.path.dirname(os.path.abspath(__file__))


def build_page(datasets):
    shell = open(os.path.join(HERE, 'standalone.html')).read()
    page = open(os.path.join(HERE, 'page.html')).read()
    return shell.replace('__DATASETS__', json.dumps(sorted(datasets))).replace('__PAGE__', page)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('out_dir')
    ap.add_argument('--bucket', default='resilentpublic')
    ap.add_argument('--prefix', default='latest/tijuana/dashboard/')
    a = ap.parse_args()
    client = Minio(os.environ['S3_ADDRESS'], access_key=os.environ['S3_ACCESS_KEY'],
                   secret_key=os.environ['S3_SECRET_KEY'], secure=os.environ.get('S3_USE_SSL', 'true').lower() != 'false')

    def put(key, body, content_type):
        data = body.encode('utf-8')
        client.put_object(a.bucket, a.prefix + key, io.BytesIO(data), len(data), content_type=content_type,
                          metadata={'Cache-Control': 'no-cache'})
        print('put', f's3://{a.bucket}/{a.prefix}{key}', len(data))

    files = sorted(glob.glob(os.path.join(a.out_dir, '*.json')))
    datasets = [os.path.basename(f)[:-5] for f in files if not f.endswith('forecast_log.json')]
    # data first, so the page never points at files that are not there yet
    for f in files:
        put('data/' + os.path.basename(f), open(f).read(), 'application/json')
    put('index.html', build_page(datasets), 'text/html; charset=utf-8')
    print('page:', f"https://{os.environ['S3_ADDRESS']}/{a.bucket}/{a.prefix}index.html")


if __name__ == '__main__':
    main()

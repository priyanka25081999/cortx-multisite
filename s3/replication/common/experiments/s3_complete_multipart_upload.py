#!/usr/bin/env python3

#
# Copyright (c) 2021 Seagate Technology LLC and/or its Affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.

import aiohttp
import asyncio
import json
import sys
import urllib
from hashlib import new
from os.path import abspath, join, dirname
from s3replicationcommon.aws_v4_signer import AWSV4Signer

# Import config module from '../tests/system'
sys.path.append(abspath(join(dirname(__file__),'..','tests', 'system')))
from config import Config

async def main():
    async with aiohttp.ClientSession() as session:
        config = Config()

        bucket_name = config.source_bucket_name
        object_name = config.object_name_prefix

        request_uri = AWSV4Signer.fmt_s3_request_uri(bucket_name, object_name)
        query_params = urllib.parse.urlencode({'uploadId': 'e0c2ee34-869a-45ef-8b19-e25ff6f548c4'})
        body = ""

        file1 = '<CompleteMultipartUpload><Part><ETag>"a3640f607e9c6d717680b0d6a0495e13"</ETag><PartNumber>1</PartNumber></Part><Part><ETag>"427698684cf0abf703fb92bb8fa9f55a"</ETag><PartNumber>2</PartNumber></Part><Part><ETag>"6eb5db36120c3bb881df0e2b16deb9d8"</ETag><PartNumber>3</PartNumber></Part><Part><ETag>"864a6873142dfa689246afd3629ffab0"</ETag><PartNumber>4</PartNumber></Part><Part><ETag>"4b64620081de89a3622732f7d60b665e"</ETag><PartNumber>5</PartNumber></Part></CompleteMultipartUpload>'
        print("file1 {}, type {}".format(file1, type(file1)))

        headers = AWSV4Signer(
            config.endpoint,
            config.s3_service_name,
            config.s3_region,
            config.access_key,
            config.secret_key).prepare_signed_header(
            'POST',
            request_uri,
            query_params,
            body)

        if (headers['Authorization'] is None):
            print("Failed to generate v4 signature")
            sys.exit(-1)

        print('POST on {}'.format(config.endpoint + request_uri))

        async with session.post(config.endpoint + request_uri,
            data=file1, params=query_params, headers=headers) as resp:
            http_status = resp.status
            print("Response of POST request {} ".format(resp))

        if http_status == 200:
            print("HTTP status {} OK!".format(http_status))
        else:
            print("ERROR : BAD RESPONSE! status = {}".format(http_status))

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

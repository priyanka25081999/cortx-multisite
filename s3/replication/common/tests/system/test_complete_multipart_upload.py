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

import asyncio
import os
import sys
from config import Config
from s3replicationcommon.log import setup_logger
from s3replicationcommon.s3_complete_multipart_upload import S3AsyncCompleteMultipartUpload
from s3replicationcommon.s3_site import S3Site
from s3replicationcommon.s3_session import S3Session


async def main():
    config = Config()

    # Setup logging and get logger
    log_config_file = os.path.join(os.path.dirname(__file__),
                                   'config', 'logger_config.yaml')

    print("Using log config {}".format(log_config_file))
    logger = setup_logger('client_tests', log_config_file)
    if logger is None:
        print("Failed to configure logging.\n")
        sys.exit(-1)

    s3_site = S3Site(config.endpoint, config.s3_service_name, config.s3_region)

    session = S3Session(logger, s3_site, config.access_key, config.secret_key)

    # Generate object name
    object_name = str(config.object_name_prefix)
    bucket_name = config.source_bucket_name
    request_id = "dummy-request-id"
    upload_id = "ff497b26-c35e-48bd-9786-fd51566e1b2c"
    e_dict = {1: '"e353b6c92564247d2d714e90db8ad65e"', 2: '"a76f33f188d4c3c89cfc9dd0b8836038"', 3: '"7f7a29962070d2b2702189acfde3e3c2"', 4: '"878b5579806c616a28c451620ba3d8ed"', 5: '"821e4f3f51a53e7b0264b744b440e71a"'}
    obj = S3AsyncCompleteMultipartUpload(session, request_id,
                                         bucket_name,object_name, 
                                         upload_id, e_dict)
    await obj.complete_upload()
    await session.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

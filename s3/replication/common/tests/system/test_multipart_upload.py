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
#

import asyncio
from config import Config
import os
import sys
from object_generator_multipart_upload import MultipartObjectDataGenerator
from s3replicationcommon.log import setup_logger
from s3replicationcommon.s3_site import S3Site
from s3replicationcommon.s3_session import S3Session
from s3replicationcommon.s3_create_multipart_upload import S3AsyncCreateMultipartUpload
from s3replicationcommon.s3_upload_part import S3AsyncUploadPart
from s3replicationcommon.s3_complete_multipart_upload import S3AsyncCompleteMultipartUpload

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

    # Generate object names
    source_object_name = str(config.object_name_prefix)
    request_id = "dummy-request-id"

    # Start Multipart
    # Create multipart
    obj_create = S3AsyncCreateMultipartUpload(session, request_id,
                                     config.source_bucket_name,
                                     source_object_name)

    # call to create method
    await obj_create.create()
    # get the upload id
    upload_id = obj_create.get_response_header("UploadId") 

    # get the data
    obj_data_generator = MultipartObjectDataGenerator(logger, config.object_size,
                                                              config.part_number)

    data = obj_data_generator.fetch()
    
    # call to upload part 
    obj_upload = S3AsyncUploadPart(session, request_id,
                                        config.source_bucket_name,
                                        source_object_name, upload_id,
                                        config.part_number)
    async for _ in data:
        for item in _:
            print("item{}".format(item["part_no"]))
            await obj_upload.upload(item)   # here pass get-range read class object

    print("***Dict{}***".format(obj_upload.get_etag_dict()))
    # this will return dict {'Part-Number': '', 'ETag': ''}

    # get the e-tag dict
    e_dict = obj_upload.get_etag_dict()

    # call to complete multipart upload
    obj_complete = S3AsyncCompleteMultipartUpload(session, request_id,
                                         config.source_bucket_name,
                                         source_object_name, 
                                         upload_id, e_dict) # pass the dict here
    await obj_complete.complete_upload() 
    print("Final ETag : {}".format(obj_complete.get_final_etag()))

    logger.info("S3AsyncMultipartUpload test passed!")

    await session.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

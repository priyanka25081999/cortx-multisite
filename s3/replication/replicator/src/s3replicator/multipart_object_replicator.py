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

import logging
from s3replicationcommon.job import JobEvents
from s3replicationcommon.s3_common import S3RequestState
from s3replicationcommon.s3_head_object import S3AsyncHeadObject
from s3replicationcommon.s3_get_object import S3AsyncGetObject
from s3replicationcommon.s3_create_multipart_upload import S3AsyncCreateMultipartUpload
from s3replicationcommon.s3_upload_part import S3AsyncUploadPart
from s3replicationcommon.s3_complete_multipart_upload import S3AsyncCompleteMultipartUpload
from s3replicationcommon.timer import Timer

_logger = logging.getLogger('s3replicator')


class MultipartObjectReplicator:
    def __init__(self, job, transfer_chunk_size_bytes, 
            source_session, target_session, 
            part_count, part_length) -> None:
        """Initialise."""
        self._transfer_chunk_size_bytes = transfer_chunk_size_bytes
        self._job_id = job.get_job_id()
        self._request_id = self._job_id
        self._timer = Timer()
        self._part_count = part_count
        self._part_length = part_length

        # A set of observers to watch for varius notifications.
        # To start with job completed (success/failure)
        self._observers = {}

        self._s3_source_session = source_session
        self._source_bucket = job.get_source_bucket_name()
        self._source_object = job.get_source_object_name()
        self._object_size = job.get_source_object_size()

        # Setup target site info
        self._s3_target_session = target_session
        self._target_bucket = job.get_target_bucket_name()

    def get_execution_time(self):
        """Return total time for Object replication."""
        return self._timer.elapsed_time_ms()

    def setup_observers(self, label, observer):
        self._observers[label] = observer

    async def start(self):
        # Start transfer
        # call head object to get information of specific part
        # Create multipart - this should happens only once for each request irrespective of part count
        self._obj_create = S3AsyncCreateMultipartUpload(self._s3_target_session, self._request_id,
                                     self._target_bucket, self._source_object)
        await self._obj_create.create()

        self._start_bytes = 0
        all_objects = []

        for part in int(self._part_length):
            print("***Part Count {} ***".format(part))

            self._object_source_reader = S3AsyncGetObject(
            self._s3_source_session,
            self._request_id,
            self._source_bucket,
            self._source_object,
            int(self._object_size),
            self._start_bytes, part) # for now -1 -1

           
            
            # get the upload id
            upload_id = self._obj_create.get_response_header("UploadId") 
            
            # Upload part
            self._obj_upload = S3AsyncUploadPart(self._s3_target_session, self._request_id,
                                            self._target_bucket,
                                            self._source_object, upload_id,
                                            self._part_count)
            
            self._timer.start()
            await self._obj_upload.upload(self._object_source_reader, part, )  # assume we are passing data here

            # get the e-tag dict
            e_dict = self._obj_upload.get_etag_dict()


        #     reader = self._object_source_reader.fetch(5000000)
        #     async for _ in reader:
        #         local_dict = {}
        #         all_objects.append({"part_no":int(part), "data":_})
        #         local_dict.clear()
        #         #print("***Data {}***".format(_))

        #     #print("***Get part etag : {}***".format(self._object_source_reader.get_etag()))
        #     self._timer.stop()
        #     #self._start_bytes += head_object.get_content_length() 
        #     self._start_bytes = self._start_bytes + part + 1
        
        # print("part no {}".format(all_objects[0]["part_no"]))
        # for _ in all_objects:
        #     print("Part No {}".format(_["part_no"]))

        #  
        
        # # 
        # call to complete multipart upload - should be outside of for loop
        self._obj_complete = S3AsyncCompleteMultipartUpload(self._s3_target_session, self._request_id,
                                         self._target_bucket,
                                         self._source_object, 
                                         upload_id, e_dict) # pass the dict here
        await self._obj_complete.complete_upload() 
        print("Final ETag : {}".format(self._obj_complete.get_final_etag()))
        _logger.info(
            "Replication of tag completed in {}ms for job_id {}".format(
                self._timer.elapsed_time_ms(), self._job_id))

        # notify job state events
        # for label, observer in self._observers.items():
        #     _logger.debug(
        #         "Notify completion to observer with label[{}]".format(label))
        #     if object_tag_writer.get_state() == S3RequestState.PAUSED:
        #         await observer.notify(JobEvents.STOPPED, self._job_id)
        #     elif object_tag_writer.get_state() == S3RequestState.ABORTED:
        #         await observer.notify(JobEvents.ABORTED, self._job_id)
        #     else:
        #         await observer.notify(JobEvents.COMPLETED, self._job_id)

  


    def pause(self):
        """Pause the running object tranfer."""
        pass  # XXX

    def resume(self):
        """Resume the running object tranfer."""
        pass  # XXX

    def abort(self):
        """Abort the running object tranfer."""
        self._object_writer.abort()

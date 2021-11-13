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
from os import urandom
from s3replicationcommon.s3_common import S3RequestState

class MultipartObjectDataGenerator:
    def __init__(self, logger, object_size, part_number):
        """Initialise."""
        self._logger = logger

        self.object_size = object_size
        self.part_number = part_number

        self._state = S3RequestState.INITIALISED

    def get_state(self):
        """Returns current request state."""
        return self._state

    async def fetch(self):
        self._state = S3RequestState.RUNNING
        
        total_chunks = int(self.object_size / self.part_number)
        start_chunk_size = 0
        part_no = 1
        all_objects = []
        print("Total chunks {}".format(total_chunks))
        
        while start_chunk_size < self.object_size:
            local_data = None
            local_data = urandom(total_chunks)

            local_dict = {}
            all_objects.append({"part_no":part_no, "data":local_data})
            local_dict.clear()

            start_chunk_size+=total_chunks
            local_data = None
            part_no+=1

        self._state = S3RequestState.COMPLETED
        yield all_objects
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

import json
from ast import literal_eval
from .prepare_job import PrepareReplicationJob
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import OffsetAndMetadata, TopicPartition

class KafkaMain:
    consumer = None

    '''
      {
        "job_id": { <topic> : <topic_name>,
                    <partision> : <partition_name>
                    <offset> : <offset_value>
                  }
       }
    '''
    job_id_to_offset = {}

    def __init__(self):
        """Initialize"""
        self._consumer_details = {}
    
    def initialize(self, app):
        self._app = app
        KafkaMain.consumer = AIOKafkaConsumer(
            'FDMI',
            bootstrap_servers='127.0.0.1:9092',
            group_id="test",
            auto_offset_reset='earliest',
            enable_auto_commit=False)
    
    def get_topic(self, job_id):
        """Returns the topic."""
        self._topic = KafkaMain.job_id_to_offset[job_id]["Topic"]
        return self._topic
    
    def get_partition(self, job_id):
        self._partition = KafkaMain.job_id_to_offset[job_id]["Partition"]
        return self._partition
    
    def get_offset(self, job_id):
        self._offset = KafkaMain.job_id_to_offset[job_id]["Offset"]
        return self._offset

    async def sendJob(self, data):
        print("Sending Jobs...")
        fdmi_dict = literal_eval(data)  
        job_record = PrepareReplicationJob.from_fdmi(fdmi_dict)

        if job_record is None:
            print("BadRequest")
            return

        jobs_list = self._app['all_jobs']
        if jobs_list.is_job_present(job_record["replication-id"]):
            # Duplicate job submitted.
            print('Duplicate Job!')
            return

        job = jobs_list.add_job_using_json(job_record)
        job_id = job.get_job_id()
        print("Added Job : {}".format(job_id))

        # Getting job here, let's do mapping
        KafkaMain.job_id_to_offset[job_id] = self._consumer_details
        print("Job to offset mapping : {}".format(KafkaMain.job_id_to_offset))
        self._consumer_details = {}
        #await self.commit_job(job_id)

    async def consume(self):
        await KafkaMain.consumer.start()
        try:
            print("In cosumer loop")
            # Consume messages
            async for msg in KafkaMain.consumer:
                obj = json.loads(msg.value.decode())
                print("consumed: ", msg.topic, msg.partition, msg.offset,
                    msg.key, obj['cr_val'], msg.timestamp)
                # self._topic = msg.topic
                # self._partition = msg.partition
                # self._offset = msg.offset

                self._consumer_details["Topic"] = msg.topic
                self._consumer_details["Partition"] = msg.partition
                self._consumer_details["Offset"] = msg.offset
                print(self._consumer_details)
                await self.sendJob(obj['cr_val'])
        except Exception as e:
            print("Exception : {}".format(e))
        finally:
            await KafkaMain.consumer.stop()

    async def commit_job(self, job_id):
        """Manual commit to Kafka."""
        tp = TopicPartition(self.get_topic(job_id), self.get_partition(job_id))
        #tp = TopicPartition(self._topic, self._partition)
        # OffsetAndMetadata(Offset, Metadata)
        #offsets = {tp: OffsetAndMetadata(self._offset, '')}
        offsets = {tp: OffsetAndMetadata(self.get_offset(job_id), '')}
        await KafkaMain.consumer.commit(offsets=offsets)
        print("Committed offset to kafka : {}".format(await KafkaMain.consumer.committed(tp)))
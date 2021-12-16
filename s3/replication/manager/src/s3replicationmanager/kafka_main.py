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
import json
import kafka
import requests
from kafka import KafkaConsumer

class KafkaMain:
    def __init__(self):
        
        """Initilization of Kafka"""
        print("Initializing Kafka consumer")
        self._consumer = KafkaConsumer("FDMI",
                         bootstrap_servers="127.0.0.1:9092",
                         group_id='test',
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         api_version=(2,13))
        

    async def fetch_jobs(self):
        """Fetching Jobs"""
        print("\nFetching jobs\n")
        #self.manager_start()
        try:
           for msg in self._consumer:
                obj = json.loads(msg.value.decode())
                print(obj)
                print("-------\n value :  {}".format(obj['cr_val']))
                print("Hey, Job is here")
                yield obj['cr_val']
                #resp = requests.post('http://127.0.0.1:8080' + '/jobs', json=obj)
        except Exception as e:
            print("exception {}".format(e))

    async def start(self):
        print("Start consumer")
        self._consumer.subscribe(['FDMI'])
        #self.fetch_jobs()

    def stop(self):
        print("Stop consumer")
        self._consumer.close()
    
   

import json
from ast import literal_eval
from .prepare_job import PrepareReplicationJob
from aiokafka import AIOKafkaConsumer
import asyncio
import requests

class KafkaMain:
    def __init__(self, app):
        self._app = app

    async def sendJob(self, data):
        #resp = requests.post('http://127.0.0.1:8080' + '/jobs', json=data)
        print("Got Data")
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
        print("Added Job")   


    async def consume(self):
        consumer = AIOKafkaConsumer(
            'FDMI',
            bootstrap_servers='127.0.0.1:9092',
            group_id="test")
        # Get cluster layout and join group `my-group`
        await consumer.start()
        try:
            print("In cosumer loop")
            # Consume messages
            async for msg in consumer:
                obj = json.loads(msg.value.decode())
                print("consumed: ", msg.topic, msg.partition, msg.offset,
                    msg.key, obj['cr_val'], msg.timestamp)
                await self.sendJob(obj['cr_val'])

        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()

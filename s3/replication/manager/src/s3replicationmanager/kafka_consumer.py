
from aiokafka import AIOKafkaConsumer
import asyncio

async def consume():
    consumer = AIOKafkaConsumer(
        'FDMI',
        bootstrap_servers='127.0.0.1:9092',
        group_id="test")
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            obj = json.loads(msg.value.decode())
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, obj, msg.timestamp)
            resp = requests.post('http://127.0.0.1:8080' + '/jobs', json=obj)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


loop = asyncio.get_event_loop()
loop.run_until_complete(consume())
#consumer = AIOKafkaConsumer("FDMI",
 #                        bootstrap_servers="127.0.0.1:9092",
  #                       group_id='test',
   #                      auto_offset_reset='earliest',
    #                     enable_auto_commit=False,
     #                    api_version=(2,13))
#consumer.subscribe(['FDMI'])
#await consumer.start()
#try:
 #    async for msg in consumer:
  #      obj = json.loads(msg.value.decode())
   #     print("Kafka offset :{}  {}".format(msg.partition,msg.offset))
    #    print("Event consumed :  {}".format(obj['cr_val']))
     #   #resp = requests.post('http://127.0.0.1:8080' + '/jobs', json=obj['cr_val'])
        #commit_dict[msg.partition] = msg.offset
        #consumer.commit(commit_dict)
#except Exception as e:
     #print("exception {}".format(e))

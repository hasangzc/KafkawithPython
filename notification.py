import json
from kafka import KafkaConsumer


NOTIFICATION_TOPIC = 'notifications'

consumer = KafkaConsumer(NOTIFICATION_TOPIC ,
                        bootstrap_servers="localhost:29092",
                        api_version=(0, 10, 1))

print("Start Listening....")
while True:
    for message in consumer:
        c_message =  json.loads(message.value.decode())
        # c_message = json.dumps(c_message)
        for i in c_message:
            if f"{i['remaining_time'][1:2]}" == ":":
                print(f"{i['remaining_time'][:1]} hours until the start of the match between the {i['home']} and {i['away']}! ")
            if f"{i['remaining_time'][2:3]}" == ":":
                print(f"{i['remaining_time'][:2]} hours until the start of the match between the {i['home']} and {i['away']}! ")
            print("---------------------------------------------------")
        # print(c_message)
import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, date, timedelta



MATCH_KAFKA_TOPIC = 'match_details'
NOTIFICATION_TOPIC = 'notifications'

consumer = KafkaConsumer(MATCH_KAFKA_TOPIC,
                        bootstrap_servers="localhost:29092",
                        api_version=(0, 10, 1))


producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Start Listening....")
while True:
    for message in consumer:
        c_message =  json.loads(message.value.decode())
        c_message = json.dumps(c_message)
        with open("sample.json", "w") as outfile:
            outfile.write(c_message)
        print("incoming data....")
        data = json.load(open('sample.json'))
        print(data)
        for i in data['upcoming_mathces']:
            i['time'] = datetime.strptime(i['time'], '%H:%M').time()
            tomorrow = date.today() + timedelta(1)
            current_dateTime = datetime.now()
            i['time'] = datetime.combine(tomorrow, i['time'])
            i['remaining_time'] =  i['time'] - current_dateTime
        print("------------------------------")
        data = data['upcoming_mathces']
        producer.send(NOTIFICATION_TOPIC, json.dumps(data, default = str).encode("utf-8"))
        producer.flush()
        print("Done Sending......")
    


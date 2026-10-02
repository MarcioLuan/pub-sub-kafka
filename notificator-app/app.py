import telebot
import os
from confluent_kafka import Consumer, KafkaError
import json
import logging

API_KEY = "8691649587:AAHWrO1K6MQbTZ7E2maNjmdJ8zg9W-pxmco"
CHAT_ID = "1062671577"

bot = telebot.TeleBot(API_KEY)

### Consumer
c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notificator-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['notification'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            texto = data['message']
            logging.warning(f"NOTIFICANDO: {texto}")
            bot.send_message(CHAT_ID, texto)
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            logging.error('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()
import pika
import time
import random

def on_message_received(ch, method, properties, body):
    processing_time = 4
    print(f'C2 received: "{body}", will take {processing_time} to process')
    time.sleep(processing_time)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f'C2 finished processing and acknowledged message')

connection_parameters = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel()

channel.queue_declare(queue='letterbox')

channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue='letterbox', on_message_callback=on_message_received)

print('Starting Consuming 2 ')

channel.start_consuming()
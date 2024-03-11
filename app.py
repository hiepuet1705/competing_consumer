import pika
import time
import tkinter as tk
import threading
connection_parameters = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()
def on_message_received(ch, method, properties, body, consumer, text_widget):
    processing_time = 3
    message = f'{consumer} received: "{body}", will take {processing_time} to process\n'
    text_widget.insert(tk.END, message)
    text_widget.see(tk.END)
    time.sleep(processing_time)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    message = f'{consumer} finished processing and acknowledged message {body}\n'
    text_widget.insert(tk.END, message)
    text_widget.see(tk.END)

def send_message():
    channel.queue_purge(queue='letterbox')
    messageId = 1
    while True:
        message = f"Sending Message Id: {messageId}"
        channel.basic_publish(exchange='', routing_key='letterbox', body=message)
        producer_text_widget.insert(tk.END, f"sent message: {message}\n")
        producer_text_widget.see(tk.END)
        time.sleep(2)
        messageId += 1

def start_consumer(consumer, text_widget):
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()
    channel.queue_declare(queue='letterbox')
    channel.basic_qos(prefetch_count=1)
    text_widget.insert(tk.END, f'Starting Consumer {consumer}\n')
    text_widget.see(tk.END)
    channel.basic_consume(queue='letterbox', on_message_callback=lambda ch, method, properties, body: on_message_received(ch, method, properties, body, consumer, text_widget))
    channel.start_consuming()

def start_producer():
    producer_thread = threading.Thread(target=send_message)
    producer_thread.daemon = True
    producer_thread.start()

def start_consumers():
    consumer_thread_1 = threading.Thread(target=lambda: start_consumer("C1", consumer_text_widget_1))
    consumer_thread_1.daemon = True
    

    consumer_thread_2 = threading.Thread(target=lambda: start_consumer("C2", consumer_text_widget_2))
    consumer_thread_2.daemon = True
    consumer_thread_3 = threading.Thread(target=lambda: start_consumer("C3", consumer_text_widget_3))
    consumer_thread_3.daemon = True
    consumer_thread_1.start()
    consumer_thread_2.start()
    consumer_thread_3.start()

# Tkinter GUI
root = tk.Tk()
root.title("Competing Consumer Demo")
start_button = tk.Button(root, text="Start", command=lambda: [start_producer(), start_consumers()])

producer_frame = tk.Frame(root)
producer_frame.pack(side="left")

producer_label = tk.Label(producer_frame, text="Producer:")
producer_label.pack()

producer_text_widget = tk.Text(producer_frame, height=20, width=80)
producer_text_widget.config(background="black", foreground="white")
producer_text_widget.pack()

# Widget consumers
consumers_frame = tk.Frame(root)
consumers_frame.pack(side="right")

consumer_label_1 = tk.Label(consumers_frame, text="Consumer 1:")
consumer_label_1.pack()

consumer_text_widget_1 = tk.Text(consumers_frame, height=12, width=80)
consumer_text_widget_1.config(background="black", foreground="white")

consumer_text_widget_1.pack()

consumer_label_2 = tk.Label(consumers_frame, text="Consumer 2:")
consumer_label_2.pack()

consumer_text_widget_2 = tk.Text(consumers_frame, height=12, width=80)
consumer_text_widget_2.config(background="black", foreground="white")

consumer_text_widget_2.pack()

consumer_label_3 = tk.Label(consumers_frame, text="Consumer 3:")
consumer_label_3.pack()

consumer_text_widget_3 = tk.Text(consumers_frame, height=12, width=80)
consumer_text_widget_3.config(background="black", foreground="white")

consumer_text_widget_3.pack()

start_button.pack()

root.mainloop()

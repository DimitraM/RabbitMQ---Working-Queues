# work queues using python and rabbitmq

import pika
import sys , time , random

# Get the message from command-line arguments, or use a default message
message = " ".join(sys.argv[1:]) or 'Default Message Body'

def generate_id():
    #Random Id
    return str(random.randint(1000, 9999))

def send_message(channel, message):
    channel.basic_publish(
        exchange="",
        routing_key="task_queue",
        body=message,
        properties=pika.BasicProperties(delivery_mode=2) #making the message persistent so it will survive a restart
         
    )
    #print item produced
    print("Item: ",message)

def producer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue="task_queue", durable=True) #durable -> exchange will survive a RabbitMQ restart


    # Generate and send messages
    while True:
        message_id = generate_id()
        message_body = f"Message ID: {message_id}, Produced Time: {time.time()}\nMessage Body: {message}"
        send_message(channel, message_body)
        time.sleep(random.randint(1.0,2.0))  # Simulate some random delays between messages(1-2seconds)

    # Close the connection
    connection.close()


if __name__=="__main__":
    producer()
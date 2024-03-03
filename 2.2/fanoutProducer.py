import pika
import random
import time , sys

def generate_id():
    #Δημιουργία τυχαίου Id
    return str(random.randint(1000, 9999))

# Get the message from command-line arguments, or use a default message
message = " ".join(sys.argv[1:]) or 'Default Message Body'

def send_message(channel, message):
    properties = pika.BasicProperties(
    message_id=generate_id()  # Set message_id explicitly
    )
    
    # Publish message
    channel.basic_publish(exchange='fannout_exchange', routing_key='fannout', body=message,properties=properties)
    print(f" Item Sent: {message}")


def producer():
    # Connect to RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the exchange
    channel.exchange_declare(exchange='fannout_exchange', exchange_type='fanout')

    # Generate and send messages
    while True:
        message_id = generate_id()
        message_body = f"Message ID: {message_id}, Produced Time: {time.time()}\nMessage Body: {message}"
        send_message(channel, message_body)
        time.sleep(random.randint(1.0,2.0))  # Simulate some random delays between messages(1-2seconds)

    # Close the connection
    connection.close()

if __name__ == "__main__":
    producer()

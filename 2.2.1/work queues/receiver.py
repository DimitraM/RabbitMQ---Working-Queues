import pika, sys, os, time, random
from datetime import datetime
#consumer
def main():
#Functions
    def generate_unique_id():
        return str(random.randint(1000, 9999))

    def get_current_time():
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]    
    
    def callback(ch,method,properties,body):
        message_id = generate_unique_id()
        current_time = get_current_time()
        print(f"Received message ID: {message_id},\nTime: {current_time}")
        print(f"Received time:",get_current_time())
        print(f"Message Body:" ,body.decode())
        time.sleep(body.count(b'.'))
        print("--------")
        #aknowledge successfull processing with delivery tag
        ch.basic_ack(delivery_tag=method.delivery_tag)


    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel() #connected to broker

    #queue declaration 
    #durable = True - ensures that the queue and its messages persist across broker restarts,
    channel.queue_declare(queue='task_queue',durable=True)

    #channel.basic_qos(prefetch_count=1)  # Ensure only one message is processed at a time for data loss prevention
    channel.basic_consume(queue='task_queue',on_message_callback=callback)

    #And finally, we enter a never-ending loop that waits for data and runs 
    #callbacks whenever necessary, and catch KeyboardInterrupt during program shutdown.
    print(' [*] Waiting for messages. To exit press CTRL+C')

    channel.start_consuming()
    
if __name__=='__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
# work queues using python and rabbitmq

import pika
import sys , time , random  
from progress.bar import Bar

# Get the message from command-line arguments, or use a default message
message = " ".join(sys.argv[1:]) or 'Default Message Body'

def generate_id():
    try:
        inputId = input("ENTER Id for this Task or PRESS any key to random generate it... : ") 
        intInputId = int(inputId)
        if len(inputId) !=4 : 
            raise ValueError("Input must be 4-digit integer")
    except:
        intInputId =str(random.randint(1000, 9999))
    #Return Id
    return intInputId

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
        effortTime = input("ENTER Time for this task's effort or PRESS any key to random generate it... : ")
        try:
            
            taskTime = int(effortTime)# trying to convert str to int, if success then it is an effort
            if taskTime <=0:
                 raise ValueError("Duration must be a positive integer")
        except: # else they want to continue randomly
            taskTime = random.randint(1,5)
        message_id = generate_id()
        message_body = f"Task Effort: {taskTime}, Message ID: {message_id}, Produced Time: {time.time()}\nMessage Body: {message}"
        send_message(channel, message_body)

        with Bar('Processing..',suffix='%(percent).1f%% - %(eta)ds') as bar:
            for i in range(100):
                time.sleep(taskTime/100)  # Delay message for the effort the task needs
                bar.next()
    # Close the connection
    connection.close()


if __name__=="__main__":
    producer()
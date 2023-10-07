import zmq

def listen_for_acks():
    context = zmq.Context()
    
    # Create a SUB socket to subscribe to the publisher
    subscriber = context.socket(zmq.SUB)
    
    # Bind the SUB socket to the specified address
    subscriber.bind("tcp://127.0.0.1:5558")
    
    # Set the subscription option to receive all messages (an empty string subscribes to all topics)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
    
    try:
        while True:
            # Receive and print acknowledgement messages
            ack_msg = subscriber.recv_string()
            print(f"Received Ack: {ack_msg}")
    except KeyboardInterrupt:
        print("Stopped listening for acks.")
    finally:
        # Close the ZMQ subscriber socket
        subscriber.close()

# Call the function to start listening for acks
listen_for_acks()

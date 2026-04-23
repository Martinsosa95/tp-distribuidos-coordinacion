import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class GracefulExit(Exception):
    pass

def signal_handler(signum, frame):
    raise GracefulExit()

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.clients_data = {}
        self.agg_counts = {}

    def _process_partial_top(self, client_id, partial_top_data):
        if client_id not in self.clients_data:
            self.clients_data[client_id] = {}
            
        for fruit, amount in partial_top_data:
            self.clients_data[client_id][fruit] = self.clients_data[client_id].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
                
        self.agg_counts[client_id] = self.agg_counts.get(client_id, 0) + 1
        
        if self.agg_counts[client_id] == AGGREGATION_AMOUNT:
            if client_id in self.clients_data:
                fruit_list = list(self.clients_data[client_id].values())
                fruit_list.sort()
                
                fruit_chunk = fruit_list[-TOP_SIZE:]
                fruit_chunk.reverse()
                final_top = [(item.fruit, item.amount) for item in fruit_chunk]
                
                del self.clients_data[client_id]
            else:
                final_top = []
                
            payload = {
                "client_id": client_id,
                "data": final_top
            }
            self.output_queue.send(message_protocol.internal.serialize(payload))
            del self.agg_counts[client_id]

    def process_message(self, message, ack, nack):
        try:
            payload = message_protocol.internal.deserialize(message)
            client_id = payload["client_id"]
            msg_type = payload.get("type")
            
            if msg_type == "TOP_PARCIAL":
                self._process_partial_top(client_id, payload["data"])
                
            ack()
        except Exception as e:
            logging.error(f"Error procesando el mensaje en Joiner: {e}")
            nack()

    def start(self):
        logging.info("Iniciando consumo de mensajes...")
        self.input_queue.start_consuming(self.process_message)

    def close(self):
        logging.info("Cerrando conexiones del Joiner...")
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    signal.signal(signal.SIGTERM, signal_handler)
    
    join_filter = JoinFilter()
    try:
        join_filter.start()
    except GracefulExit:
        logging.info("Recibida señal SIGTERM. Apagando JoinFilter de forma segura.")
    finally:
        join_filter.close()

    return 0


if __name__ == "__main__":
    main()

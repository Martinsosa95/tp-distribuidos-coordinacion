import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
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

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.clients_data = {}

        self.eof_counts = {}

    def _process_data(self, client_id, fruit, amount):
        if client_id not in self.clients_data:
            self.clients_data[client_id] = {}
            
        self.clients_data[client_id][fruit] = self.clients_data[client_id].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        self.eof_counts[client_id] = self.eof_counts.get(client_id, 0) + 1
        
        if self.eof_counts[client_id] == SUM_AMOUNT:
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
                "type": "TOP_PARCIAL",
                "data": final_top
            }
            
            self.output_queue.send(message_protocol.internal.serialize(payload))
            del self.eof_counts[client_id]

    def process_message(self, message, ack, nack):
        try:
            payload = message_protocol.internal.deserialize(message)
            client_id = payload["client_id"]
            msg_type = payload["type"]

            if msg_type == "DATA":
                [fruit, amount] = payload["data"]
                self._process_data(client_id, fruit, amount)
            elif msg_type == "EOF":
                self._process_eof(client_id)
                
            ack()
        except Exception as e:
            logging.error(f"Error procesando el mensaje: {e}")
            nack()

    def start(self):
        logging.info("Iniciando consumo de mensajes...")
        self.input_exchange.start_consuming(self.process_message)

    def close(self):
        logging.info("Cerrando conexiones...")
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    signal.signal(signal.SIGTERM, signal_handler)
    
    aggregation_filter = AggregationFilter()
    try:
        aggregation_filter.start()
    except GracefulExit:
        logging.info("Recibida señal SIGTERM. Apagando AggregationFilter de forma segura.")
    finally:
        aggregation_filter.close()
        
    return 0


if __name__ == "__main__":
    main()

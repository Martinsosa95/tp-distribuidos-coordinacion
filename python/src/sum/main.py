import os
import logging
import signal
import hashlib
import time

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class GracefulExit(Exception):
    pass

def signal_handler(signum, frame):
    raise GracefulExit()

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit = {}
        self.clients_data = {}

    def _flush_client_data(self, client_id):
        if client_id not in self.clients_data:
            return

        for fruit_name, f_item in self.clients_data[client_id].items():
            hash_val = int(hashlib.md5(fruit_name.encode('utf-8')).hexdigest(), 16)
            target_agg_idx = hash_val % AGGREGATION_AMOUNT
            
            payload = {
                "client_id": client_id,
                "type": "DATA",
                "data": [f_item.fruit, f_item.amount]
            }
            self.data_output_exchanges[target_agg_idx].send(
                message_protocol.internal.serialize(payload)
            )
        del self.clients_data[client_id]
    
    def _send_eof_to_aggregators(self, client_id):
        """Notifica a todos los agregadores que este cliente finalizó"""
        payload = {
            "client_id": client_id,
            "type": "EOF"
        }
        serialized_eof = message_protocol.internal.serialize(payload)
        for exchange in self.data_output_exchanges:
            exchange.send(serialized_eof)

    def _process_eof(self):
        logging.info(f"Broadcasting data messages")
        for final_fruit_item in self.amount_by_fruit.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([]))


    def process_data_message(self, message, ack, nack):
        try:
            payload = message_protocol.internal.deserialize(message)
            client_id = payload["client_id"]
            msg_type = payload["type"]

            if msg_type == "DATA":
                [fruit, amount] = payload["data"]
                
                if client_id not in self.clients_data:
                    self.clients_data[client_id] = {}
                
                self.clients_data[client_id][fruit] = self.clients_data[client_id].get(
                    fruit, fruit_item.FruitItem(fruit, 0)
                ) + fruit_item.FruitItem(fruit, int(amount))

            elif msg_type == "EOF":
                self._flush_client_data(client_id)
                self._send_eof_to_aggregators(client_id)
                
                if SUM_AMOUNT > 1:
                    token = {
                        "client_id": client_id,
                        "type": "EOF_SYNC",
                        "visited": [ID],
                        "bounces": 0
                    }
                    self.input_queue.send(message_protocol.internal.serialize(token))

            elif msg_type == "EOF_SYNC":
                visited = payload["visited"]
                bounces = payload.get("bounces", 0)

                if ID in visited:
                    bounces += 1
                    bounce_limit = SUM_AMOUNT * 2
                    if bounces > bounce_limit:
                        logging.warning(f"Bounce limit exceeded for client {client_id}. Possible network issue. Dropping message.")
                        dead_nodes = SUM_AMOUNT - len(visited)
                        for _ in range(dead_nodes):
                            self._send_eof_to_aggregators(client_id)
                    payload["bounces"] = bounces
                    time.sleep(0.1)
                    self.input_queue.send(message_protocol.internal.serialize(payload))
                else:
                    self._flush_client_data(client_id)
                    self._send_eof_to_aggregators(client_id)
                    visited.append(ID)

                    if len(visited) < SUM_AMOUNT:
                        payload["visited"] = visited
                        payload["bounces"] = 0
                        self.input_queue.send(message_protocol.internal.serialize(payload))

            ack()
        except Exception as e:
            logging.error(f"Error procesando el mensaje: {e}")
            nack()

    def start(self):
        logging.info("Iniciando consumo de mensajes...")
        self.input_queue.start_consuming(self.process_data_message)

    def close(self):
        logging.info("Cerrando conexiones...")
        self.input_queue.close()
        for exchange in self.data_output_exchanges:
            exchange.close()

def main():
    logging.basicConfig(level=logging.INFO)

    signal.signal(signal.SIGTERM, signal_handler)

    sum_filter = SumFilter()
    try:
        sum_filter.start()
    except GracefulExit:
        logging.info("Recibida señal SIGTERM.")
    finally:
        sum_filter.close()

    return 0


if __name__ == "__main__":
    main()

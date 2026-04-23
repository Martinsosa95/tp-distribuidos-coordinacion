import uuid

from common import message_protocol


class MessageHandler:
    def __init__(self):
        self.client_id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        payload = {
            "client_id": self.client_id,
            "type": "DATA",
            "data": [fruit, amount],
        }
        return message_protocol.internal.serialize(payload)

    def serialize_eof_message(self, message):
        payload = {
            "client_id": self.client_id,
            "type": "EOF",
        }
        return message_protocol.internal.serialize(payload)

    def deserialize_result_message(self, message):
        payload = message_protocol.internal.deserialize(message)

        if payload.get("client_id") == self.client_id:
            return payload.get("data")
        
        return None


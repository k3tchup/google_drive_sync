# handles keyring
import logging
import keyring
import base64

class Keyring:
    def __init__(self):
        self.keyring_active = False

    def store_data(self, service:str, key: str, data:str)-> bool:
        try:
            # base64 encode the data to avoid any issues storing it
            b64_data = base64.b64encode(data.encode('utf-8'))
            keyring.set_password(service, key, b64_data.decode('utf-8'))
            return True

        except Exception as err:
            logging.error("Error storing data in keyring. %s" % str(err))
            return False

    def get_data(self, service:str, key: str) -> str:
        data = ""
        try:
            b64_data = keyring.get_password(service, key)
            if b64_data is not None:
                data = base64.b64decode(b64_data.encode('utf-8')).decode('utf-8')
        except Exception as err:
            logging.error("Error retrieving data from keyring. %s" % str(err))

        return data

    def delete_data(self, service, key: str) -> bool:
        try:
            keyring.delete_password(service, key)
            return True

        except Exception as err:
            logging.error("Error deleting data from keyring. %s" % str(err))
            return False


from abc import abstractmethod


import os


class TokenAuth:
    def __init__(self, token_api_key, token_env_key=None, token_value=None):
        if token_value:
            self.token = token_value
        elif token_env_key:
            self.token = os.environ.get(token_env_key, '')
        else:
            raise RuntimeError("Unable to initialize TokenAuth as no env_key or token parameter defined")

        if not self.token or self.token == '':
            raise RuntimeError("No available API token detected!")

        self.params = {token_api_key: self.token}


class DataSource:

    @abstractmethod
    def get_data(self, *args, **kwargs):
        pass

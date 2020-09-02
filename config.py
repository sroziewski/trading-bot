import configparser


class Config(object):
    def __init__(self, section='local', name='resource/config.properties'):
        config = configparser.RawConfigParser()
        config.read(name)
        self.config = dict(config.items(section))

    def get_parameter(self, parameter):
        if parameter in self.config:
            return self.config[parameter]
        raise Exception("There is no such a key in config!")


config = Config()
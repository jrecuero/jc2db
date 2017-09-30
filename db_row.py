class DbRow(object):

    def __init__(self, **kwargs):
        self.update(**kwargs)

    def get_row(self):
        return self.__dict__

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def clone(self):
        return self.__class__(**self.__dict__)

    def __repr__(self):
        st = "\n".join(['{0}: {1}'.format(k, v) for k, v in self.get_row().items()])
        return st + '\n'

    def get_as_str(self, pattern, header_flag=False):
        if header_flag:
            return pattern.format(*self.get_row().keys())
        else:
            return pattern.format(*self.get_row().values())

class DB_ROW(object):

    def __init__(self, **kwargs):
        self.update(**kwargs)

    def getRow(self):
        return self.__dict__

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def clone(self):
        return self.__class__(**self.__dict__)

    def __repr__(self):
        st = "\n".join(['{0}: {1}'.format(k, v) for k, v in self.getRow().items()])
        return st + '\n'

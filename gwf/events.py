class Event(object):

    def __init__(self):
        self.observers = set()

    def __call__(self, *args, **kwargs):
        for observer in self.observers:
            observer(*args, **kwargs)

    def __iadd__(self, observer):
        self.observers.add(observer)
        return self

    def __isub__(self, observer):
        self.observers.discard(observer)
        return self

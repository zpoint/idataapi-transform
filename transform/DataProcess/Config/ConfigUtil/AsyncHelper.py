class AsyncGenerator(object):
    def __init__(self, items, process_func):
        self.items = items
        if hasattr(self.items, "__aiter__"):
            self.is_async = True
        else:
            self.is_async = False
            self.items = self.to_generator(items)
        self.process_func = process_func

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.is_async:
            r = await self.items.__anext__()
            return self.process_func(r)
        else:
            try:
                r = next(self.items)
                return self.process_func(r)
            except StopIteration:
                raise StopAsyncIteration

    @staticmethod
    def to_generator(items):
        for i in items:
            yield i

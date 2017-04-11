import tornado.ioloop
from tornado import gen
from tornado.web import Application

from tornado.testing import AsyncHTTPTestCase, gen_test
from utils.cache.decorators import Cached


class CachedTest(AsyncHTTPTestCase):
    def setUp(self):
        super().setUp()

    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()

    def get_app(self):
        return Application()

    @Cached(timeout=60)
    def cached_method(self, x, y):
        print('func runned')
        return x / y

    @Cached(timeout=60, work_in_background=True)
    def cached_method_bg(self, x, y):
        print('func runned')
        return x / y

    @gen_test
    async def test_cache_bg(self):
        self.cached_method_bg.invalidate(3, 2)
        res = await self.cached_method_bg(3, 2)
        self.assertEqual(res, None)
        await gen.sleep(1)
        res = await self.cached_method_bg(3, 2)
        self.assertEqual(res, 3 / 2)

    @gen_test
    def test_cache_plain(self):
        for i in range(10):
            a = yield self.cached_method(i, 2)
            print(a)
        self.cached_method.invalidate(20, 2)
        a = yield self.cached_method(20, 2)
        print(a)

    @Cached(timeout=60, is_sync=True)
    def cached_method_sync(self, x, y):
        print('func runned')
        return x / y

    def test_cache_sync(self):
        for i in range(10):
            a = self.cached_method_sync(i, 2)
            print(a)
        self.cached_method_sync.invalidate(20, 2)
        a = self.cached_method_sync(20, 2)
        print(a)
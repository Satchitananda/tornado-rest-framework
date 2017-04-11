import asyncmc
import memcache
import tornado.ioloop

from tornado.options import options

cache = memcache.Client(servers=options.MEMCACHED_SERVERS)
async_cache = asyncmc.Client(servers=options.MEMCACHED_SERVERS, loop=tornado.ioloop.IOLoop.instance())

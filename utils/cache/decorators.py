import inspect
import operator
from hashlib import sha1
from importlib import import_module

from tornado.ioloop import IOLoop

from functools import reduce
from tornado.options import options

from utils.cache import cache, async_cache
from functools import partial, wraps


def is_method(func):
    args_names = list(inspect.signature(func).parameters.keys())
    return len(args_names) and args_names[0] in ('cls', 'self')


class Cached(object):
    def __init__(self, slot_name=None, timeout=None, vary_on=None, is_sync=False, work_in_background=False,
                 default_result=None):
        self._slot_name = slot_name
        self._timeout = timeout
        self._vary_on = vary_on
        self.is_sync = is_sync
        self.work_in_background = work_in_background
        self.default_result = default_result

    def __call__(self, func):
        async def async_wrapped(*args, **kwargs):
            key = self.get_slot_name(func, args[1:] if is_method(func) else args, kwargs)
            result = await async_cache.get(key)
            # If result not in cache - saving
            if result is None:
                if self.work_in_background:
                    async def do_work():
                        res = func(*args, **kwargs)
                        await async_cache.set(key, res, self._timeout)
                    IOLoop.current().spawn_callback(do_work)
                    result = self.default_result
                else:
                    result = func(*args, **kwargs)
                    await async_cache.set(key, result, self._timeout)
            return result

        def wrapped(*args, **kwargs):
            key = self.get_slot_name(func, args[1:] if is_method(func) else args, kwargs)
            result = cache.get(key)
            # If result not in cache - saving
            if result is None:
                if self.work_in_background:
                    def do_work():
                        res = func(*args, **kwargs)
                        cache.set(key, res, self._timeout)
                    IOLoop.current().spawn_callback(do_work)
                    result = self.default_result
                else:
                    result = func(*args, **kwargs)
                    cache.set(key, result, self._timeout)
            return result

        if not self.is_sync:
            async_wrapped.invalidate = partial(self.invalidate, func)
            async_wrapped.update_cache = partial(self.update_cache, func)
            return wraps(func)(async_wrapped)
        else:
            wrapped.invalidate = partial(self.invalidate, func)
            wrapped.update_cache = partial(self.update_cache, func)
            return wraps(func)(wrapped)

    def _default_vary_on(self, func, *args, **kwargs):
        args_names = list(inspect.signature(func).parameters.keys())
        params = kwargs.copy()
        for q, arg_value in enumerate(args):
            try:
                arg_name = args_names[q]
            except IndexError:
                arg_name = 'arg_%d' % q
            if arg_name not in params:
                params[arg_name] = arg_value
        result = []

        base_path = options.get("SQL_ALCHEMY_BASE")

        if base_path:
            from sqlalchemy.inspection import inspect as a_inspect
            Base = import_module(base_path)

            for k, v in sorted(params.items(), key=lambda x: x[0]):
                result.extend([k, ''.join(map(str, a_inspect(v).identity)) if isinstance(v, Base) else v])
        else:
            for k, v in sorted(params.items(), key=lambda x: x[0]):
                result.extend([k, v])

        return result

    def get_slot_name(self, func, args, kwargs):
        params = self._vary_on(*args, **kwargs) if self._vary_on else self._default_vary_on(func, *args, **kwargs)
        return u':'.join([
            getattr(options, 'CACHE_KEY_PREFIX', getattr(options, 'KEY_PREFIX', '')),
            self._slot_name or '%s.%s' % (func.__module__, func.__qualname__),
            sha1(reduce(operator.add, map(str, params)).encode('utf-8')).hexdigest() if params else '',
        ])

    def invalidate(self, func, *args, **kwargs):
        key = self.get_slot_name(func, args, kwargs)
        if self.is_sync:
            cache.delete(key)
        else:
            async_cache.delete(key)

    def update_cache(self, func, new_value, *args, **kwargs):
        key = self.get_slot_name(func, args, kwargs)
        if self.is_sync:
            cache.set(key, new_value)
        else:
            async_cache.set(key, new_value)

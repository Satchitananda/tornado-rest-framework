from tornado.ioloop import IOLoop


def work_in_background(func):
    def wrapped(*args, **kwargs):
        def do_work():
            func(*args, **kwargs)
        IOLoop.current().spawn_callback(do_work)
    return wrapped

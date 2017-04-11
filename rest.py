import logging

from .resource import ListResource, ItemResource

logger = logging.getLogger('tornado.application')


class REST(object):
    def __init__(self, root):
        self.root = root
        self.resources = []

    def add_resource(self, resource, *urls, **kwargs):
        self.resources.append((resource, urls, kwargs))

    def resource(self, *urls, **kwargs):
        def decorator(cls):
            self.add_resource(cls, *urls, **kwargs)
            return cls
        return decorator

    def get_routes(self):
        routes = []
        for resource, urls, kwargs in self.resources:
            for url in urls:
                routes.append(('{}{}'.format(self.root, url), resource, kwargs))
        return routes

    def model_resource(self, url):
        def decorator(resource_def_class):
            """
            :type resource_def_class: type(ResourceDef)
            """
            model_class = resource_def_class.model_class
            self.add_resource(ListResource, url,
                              endpoint='{}_list'.format(model_class.__name__).lower(),
                              resource_def_class=resource_def_class,)

            self.add_resource(
                ItemResource,
                '{}/(?P<pk>\w+)'.format(url),
                endpoint='{}_item'.format(model_class.__name__).lower(),
                resource_def_class=resource_def_class,
            )

            return resource_def_class

        return decorator

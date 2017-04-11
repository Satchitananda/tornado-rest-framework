import logging

from sqlalchemy.orm.exc import NoResultFound
from decimal import Decimal

from .exceptions import ObjectNotFound
from .handlers import RESTHandler
from .query import apply_order_by
from .reqparse import ListRequestParser, ItemRequestParser
from .serialize import serialize_relations, get_include_mask, get_column_mapping, get_pk_column, \
    serialize, get_many_relations, get_column

logger = logging.getLogger('tornado.application')


class Resource(RESTHandler):
    representations = None
    method_decorators = []

    def initialize(self, *args, **kwargs):
        super().initialize(*args, **kwargs)
        self._content_type = ''

    def prepare(self):
        meth = super().prepare
        if self.method_decorators:
            for decorator in self.method_decorators:
                meth = decorator(meth)
            return meth()
        return meth()


class ResourceDef(object):
    model_class = None
    method_decorators = []
    pk_converter = 'int'
    fields_conf = {}

    def __init__(self, db):
        """
        :type db: SQLAlchemy.engine
        """
        assert self.model_class is not None
        self.db = db

    def get_query(self):
        return self.session.query(self.model_class)

    def convert_pk(self, pk):
        return pk

    def on_not_found(self, pk):
        raise ObjectNotFound()

    def before_save(self, entity):
        pass

    def after_save(self, entity, is_new):
        pass

    def process_data(self, data):
        return data

    def process_result(self, result):
        return result

    def include(self):
        return None


class BaseResource(Resource):
    """
    :type def_: ResourceDef
    """

    def initialize(self, *args, **kwargs):
        """
        :type resource_def_class: type(ResourceDef)
        """
        super().initialize(*args, **kwargs)
        resource_def_class = kwargs.get('resource_def_class')
        self.def_ = resource_def_class(self.application.db)
        self.def_.resource = self
        self.def_.logger = self.logger
        self.def_.session = self.session

        self.method_decorators = list(reversed(resource_def_class.method_decorators))
        self.list_request_parser = ListRequestParser(self.def_.model_class)
        self.item_request_parser = ItemRequestParser(self.def_.model_class)

    def serialize_relations(self, entities, include):
        return serialize_relations(self.def_.model_class, entities, get_include_mask(include),
                                   fields_conf=self.def_.fields_conf, meta=True)

    def update_fields(self, entity, payload):
        column_mapping = get_column_mapping(self.def_.model_class)
        for key, value in payload.items():
            if key in column_mapping:
                if isinstance(value, float) and get_column(self.def_.model_class, key).type.asdecimal:
                    value = Decimal(value)
                setattr(entity, column_mapping[key], value)


class ListResource(BaseResource):
    def get(self):
        args = self.list_request_parser.parse_args(self.request)
        query = self.def_.get_query().distinct()

        # apply load options
        if self.def_.include():
            options = []
            include = self.def_.include()
        else:
            options, include = args.include
        query = query.options(*options)

        # apply ordering
        orderings, ordering_joins, order_by = args.order_by
        ordered_query = apply_order_by(query, orderings, ordering_joins)

        # apply `where`
        clause, where = args.where
        filtered_query = ordered_query.filter(clause)

        # apply pagination
        limit = args.limit
        if args.count:
            limit = args.count

        paginated_query = filtered_query \
            .limit(limit) \
            .offset(args.offset)

        results = paginated_query.all()

        relations = self.serialize_relations(results, include)
        items = list(map(lambda x: serialize(x, fields_conf=self.def_.fields_conf, relations=relations), results))
        result = {str(self.def_.model_class.__table__): items}
        result.update(get_many_relations(relations))
        result = self.def_.process_result(result)
        self.success(result)

    def post(self):
        args = self.item_request_parser.parse_args(self.request, data=self.data)
        item = self.def_.model_class()
        self.session.add(item)

        data = self.def_.process_data(self.data)
        self.update_fields(item, data)

        self.def_.before_save(item)
        self.session.commit()
        self.def_.after_save(item, True)

        # apply load options
        options, include = args.include
        pk_column = get_pk_column(self.def_.model_class)
        pk = getattr(item, pk_column.key)
        query = self.def_.get_query()
        item = query.options(*options).filter(pk_column == pk).one()

        relations = self.serialize_relations([item], include)
        items = serialize(item, fields_conf=self.def_.fields_conf, relations=relations)
        result = {str(self.def_.model_class.__table__): items}
        result.update(get_many_relations(relations))
        result = self.def_.process_result(result)

        self.set_status(201)
        self.success(result)


class ItemResource(BaseResource):
    def get(self, pk):
        args = self.item_request_parser.parse_args(self.request)

        query = self.def_.get_query()

        # apply load options
        options, include = args.include
        query = query.options(*options)

        pk_column = get_pk_column(self.def_.model_class)
        pk = self.def_.convert_pk(pk)

        try:
            item = query.filter(pk_column == pk).one()
        except NoResultFound:
            item = self.def_.on_not_found(pk)

        relations = self.serialize_relations([item], include)
        items = serialize(item, fields_conf=self.def_.fields_conf, relations=relations)
        result = {str(self.def_.model_class.__table__): items}
        result.update(get_many_relations(relations))
        result = self.def_.process_result(result)
        self.success(result)

    def put(self, pk):
        args = self.item_request_parser.parse_args(self.request, data=self.data)

        query = self.def_.get_query()

        pk_column = get_pk_column(self.def_.model_class)
        pk = self.def_.convert_pk(pk)

        try:
            item = query.filter(pk_column == pk).one()
        except NoResultFound:
            item = self.def_.on_not_found(pk)

        data = self.def_.process_data(self.data)
        self.update_fields(item, data)

        self.def_.before_save(item)
        self.session.commit()
        self.def_.after_save(item, False)

        # apply load options
        options, include = args.include
        item = query.options(*options).filter(pk_column == pk).one()

        relations = self.serialize_relations([item], include)
        items = serialize(item, fields_conf=self.def_.fields_conf, relations=relations)
        result = {str(self.def_.model_class.__table__): items}
        result.update(get_many_relations(relations))
        result = self.def_.process_result(result)
        self.success(result)

import json
from collections import defaultdict
from datetime import datetime, date
from decimal import Decimal

from sqlalchemy.orm import class_mapper, joinedload
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.orm.base import MANYTOMANY, ONETOMANY, MANYTOONE

from .query import get_filter_expr, get_order_by


def is_column(attr):
    """
    :type attr: sqlalchemy.orm.attributes.InstrumentedAttribute
    """
    return hasattr(attr, 'property') and isinstance(attr.property,
                                                    ColumnProperty)


def is_association_proxy(attr):
    return isinstance(attr, AssociationProxy)


def get_column_mapping(model_class):
    """
    Return mapping from JSON fields (column names) to model class attribute
     names

    :type model_class: sqlalchemy.ext.declarative.api.DeclarativeMeta
    :rtype: dict
    """
    descriptors = class_mapper(model_class).all_orm_descriptors

    ret = {}

    for key, column in descriptors.items():
        if is_association_proxy(column):
            field = key
        elif is_column(column):
            field = column.key
        else:
            continue

        ret[field] = key

    return ret


def serialize(model, fields_conf=None, relations=None):
    """Transforms a model into a dictionary which can be dumped to JSON."""

    ret = {}
    if not fields_conf:
        fields_conf = {}

    if not relations:
        relations = {}

    model_class = model.__class__
    exclude_fields = fields_conf.get('exclude', {}).get(model_class.__name__, [])
    fields = fields_conf.get(model_class.__name__, [])
    column_mapping = get_column_mapping(model_class)

    for field, attr in column_mapping.items():
        value = getattr(model, attr)

        if is_association_proxy(getattr(model_class, attr)):
            value = list(value)

        if isinstance(value, (datetime, date)):
            value = value.isoformat()
        elif isinstance(value, Decimal):
            value = float(value)

        if field not in exclude_fields:
            if fields:
                if field in fields:
                    ret[field] = value
            else:
                ret[field] = value

    item = ret
    for k, v in relations.items():
        meta = v.get('_meta')
        if meta.get('fk') and meta.get('fk_pair'):
            if meta['o2m'] and not meta['o2o']:
                rel_items = v.get('_items', [])
                for rel_item in rel_items:
                    if item.get(meta['fk']) == rel_item.get(meta['fk_pair']):
                        item.setdefault(k, []).append(rel_item)

            elif meta['o2o']:
                rel_items = v.get('_items', [])
                for rel_item in rel_items:
                    rel_item = rel_item.copy()
                    if item.get(meta['fk']) == rel_item.get(meta['fk_pair']):
                        item.pop(meta['fk'])
                        item[k] = rel_item

    return item


def find_association_proxy(model_class, relation):
    descriptors = class_mapper(model_class).all_orm_descriptors
    for key, desc in descriptors.items():
        if not is_association_proxy(desc):
            continue

        try:
            if desc.local_attr is relation:
                return key
        except AttributeError:
            return None

    raise ValueError('Could not find association proxy for relation '
                     '{}'.format(relation))


def serialize_relations(model_class, entities, include_mask, fields_conf=None, meta=False):
    if not fields_conf:
        fields_conf = {}

    relations = {}
    for name in include_mask:
        relation_attr = getattr(model_class, name)
        relation_class = relation_attr.property.mapper.class_
        relation_pk = get_pk_column(relation_class)

        is_o2o = False
        is_m2m = relation_attr.property.direction is MANYTOMANY
        is_m2o = relation_attr.property.direction == MANYTOONE
        uselist = getattr(relation_attr.property, 'uselist')

        if not is_m2m:
            is_o2o = uselist is False

        fk_pair = None
        if is_m2m:
            foreign_key = find_association_proxy(model_class, relation_attr)
        else:
            foreign_keys = [column.key for column in relation_attr.property.local_columns]
            foreign_key = foreign_keys[0] if len(foreign_keys) == 1 else foreign_keys
            # getting our fk pair on remote side
            pairs = relation_attr.property.local_remote_pairs
            try:
                fk_pair = list(
                    filter(lambda x: len(x) > 1 and x[0] is not None and x[0].name == foreign_key, pairs)
                )[0][1].name
            except IndexError:
                pass

        relation_items = set()
        for item in entities:
            relation_item = getattr(item, name)
            if relation_item is not None:
                if isinstance(relation_item, list):
                    relation_items.update(relation_item)
                else:
                    relation_items.add(relation_item)

        mask = include_mask[name]
        rels = serialize_relations(relation_class, relation_items, mask, fields_conf=fields_conf, meta=True)
        items = list(map(lambda x: serialize(x, fields_conf=fields_conf, relations=rels), relation_items))

        relations[name] = {
            '_items': items,
            '_relations': rels
        }

        if meta:
            relations[name].update({'_meta': {
                'pk': relation_pk.key,
                'fk': foreign_key,
                'fk_pair': fk_pair,
                'm2m': is_m2m,
                'o2o': is_o2o,
                'm2o': is_m2o,
                'o2m': relation_attr.property.direction == ONETOMANY,
            }})

    return relations


def get_many_relations(serialized_relations):
    many_rels = {}
    for k, v in serialized_relations.items():
        if v['_meta']['m2m'] or (v['_meta']['m2o'] and not v['_meta']['o2o']):
            vals = many_rels.setdefault(k, [])
            for item in v['_items']:
                if item not in vals:
                    vals.append(item)

        rel = v.get('_relations')
        if rel:
            new_rel = get_many_relations(rel)
            for rk, rv in new_rel.items():
                values = many_rels.setdefault(rk, [])
                for item in rv:
                    if item not in values:
                        values.append(item)
    return many_rels


def get_column(model_class, name):
    """
    :type model_class: sqlalchemy.ext.declarative.api.DeclarativeMeta
    :type name: str
    """
    columns = class_mapper(model_class).columns

    for column in columns.values():
        if column.key == name:
            return column

    raise KeyError(name)


def get_pk_column(model_class):
    """
    :type model_class: sqlalchemy.ext.declarative.api.DeclarativeMeta
    :rtype: sqlalchemy.Column
    """
    pk_columns = class_mapper(model_class).primary_key

    if len(pk_columns) != 1:
        raise ValueError('Ambiguous primary key')

    return pk_columns[0]


def where_type(model_class):
    """
    Construct `WHERE` clause by query dict

    :type model_class: sqlalchemy.ext.declarative.api.DeclarativeMeta
    """

    def parse(string):
        """
        :type string: basestring
        :rtype: (sqlalchemy.sql.elements.ClauseElement, dict)
        """
        where = json.loads(string)
        if not isinstance(where, dict):
            raise TypeError('Must be object')

        return get_filter_expr(model_class, where), where

    return parse


def order_by_type(model_class):
    """
    :type model_class: sqlalchemy.ext.declarative.api.DeclarativeMeta
    """

    def parse(string):
        parsed = json.loads(string)
        orderings, joins = get_order_by(model_class, parsed)
        return orderings, joins, parsed

    return parse


def non_negative_int(string):
    number = int(string)

    if number < 0:
        raise ValueError('Must be non-negative integer')

    return number


def included_relations_type(model_class):
    """
    :type model_class: sqlalchemy.ext.declarative.api.DeclarativeMeta
    """
    def parse(string):
        """
        :type string: str
        :rtype: (list of sqlalchemy.orm.strategy_options.Load, list of str)
        """
        if not isinstance(string, (dict, list)):
            parsed = json.loads(string)
        else:
            parsed = string

        if (not isinstance(parsed, list) or
                any(not isinstance(item, str) for item in parsed)):
            raise ValueError('Must be list of strings')

        relationships = class_mapper(model_class).relationships
        options = []

        try:
            for item in parsed:
                parts = item.split('.')
                relationship = relationships[parts[0]]
                load = joinedload(relationship.class_attribute)

                for part in parts[1:]:
                    sub_relationships = relationship.mapper.relationships
                    relationship = sub_relationships[part]

                    load = load.joinedload(relationship.class_attribute)

                options.append(load)

        except KeyError as e:
            raise ValueError("No such relation: '{}'".format(e))

        return options, parsed

    return parse


def get_include_mask(include):
    """
    Transform lists of form
    ['a', 'b.c', 'b.d']
    to dict
    {'a': {}, 'b': {'c': {}, 'd': {}}}

    :type include: list of str
    """
    mask = {}

    pre_mask = defaultdict(set)

    for item in include:
        parts = item.split('.', 1)
        if len(parts) == 1:
            mask[item] = {}
        else:
            pre_mask[parts[0]].add(parts[1])

    for key, values in pre_mask.items():
        mask[key] = get_include_mask(values)

    return mask

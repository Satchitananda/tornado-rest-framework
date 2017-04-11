from functools import partial
import operator
from sqlalchemy.ext.associationproxy import AssociationProxy

import sqlalchemy.orm.interfaces
from sqlalchemy import select
from sqlalchemy.orm import ColumnProperty, aliased
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql import ColumnElement, func
from sqlalchemy.sql.expression import or_, and_, not_
from sqlalchemy_utils.functions.sort_query import make_order_by_deterministic

primitive_operators = {
    '$eq': operator.eq,
    '$ne': operator.ne,
    '$lt': operator.lt,
    '$lte': operator.le,
    '$gt': operator.gt,
    '$gte': operator.ge,
    '$in': lambda attr, operand: attr.in_(operand),
    '$nin': lambda attr, operand: ~attr.in_(operand),
    '$like': lambda attr, operand: attr.ilike(operand)
}


def length_operator(attr, criteria):
    if not isinstance(criteria, dict):
        criteria = {'$eq': criteria}

    if isinstance(attr, AssociationProxy):
        attr = attr.local_attr

    return get_attribute_filter_expr(
        select([func.count('*')]).where(attr).as_scalar(),
        criteria, key='{}$length'.format(attr.key)
    )


# *-to-many relationship operators
relationship_operators = {
    '$any': lambda attr, criteria: attr.any(
        get_filter_expr(attr.property.mapper.class_, criteria)
    ),
    '$all': lambda attr, criteria: not_(attr.any(
        not_(get_filter_expr(attr.property.mapper.class_, criteria))
    )),
    '$length': length_operator,
}

association_proxy_operators = {
    '$any': lambda attr, criteria: attr.any(
        get_attribute_filter_expr(attr.remote_attr, criteria)
    ),
    '$all': lambda attr, criteria: not_(attr.any(
        not_(get_attribute_filter_expr(attr.remote_attr, criteria))
    )),
    '$length': length_operator,
}

logical_operators = {
    '$or': lambda fn, clauses: or_(fn(item) for item in clauses),
    '$and': lambda fn, clauses: and_(fn(item) for item in clauses),
    '$not': lambda fn, clause: not_(fn(clause)),
}


def get_attribute_filter_expr(attribute, criteria, key=None):
    if key is None:
        key = attribute.key

    exprs = []
    for op, operand in criteria.items():
        if op in primitive_operators:
            if not ((hasattr(attribute, 'property') and isinstance(
                attribute.property, ColumnProperty)) or
                        isinstance(attribute, ColumnElement)):
                raise ValueError("Can't use primitive operator '{}' on '{}': "
                                 "not a column".format(op, key))
            if isinstance(attribute, ColumnElement):
                # If attribute is a BinaryExpression, we must surround it with
                # parenthesis. Otherwise, for expression `a = b` and criteria
                # `{$eq: True}` this method would return `a = b = true` instead
                # of `(a = b) = true`.
                attribute = attribute.self_group()

            expr = primitive_operators[op](attribute, operand)

        elif op in relationship_operators:
            if isinstance(attribute, AssociationProxy):
                expr = association_proxy_operators[op](attribute, operand)
            elif (isinstance(attribute.property, RelationshipProperty) and
                          attribute.property.direction in
                          (sqlalchemy.orm.interfaces.ONETOMANY,
                           sqlalchemy.orm.interfaces.MANYTOMANY)):
                expr = relationship_operators[op](attribute, operand)
            else:
                raise ValueError(
                    "Can't use '{}' operator on '{}': "
                    "not a *-to-many relation nor association "
                    "proxy".format(op, key)
                )

        elif op in logical_operators:
            expr = logical_operators[op](
                partial(get_attribute_filter_expr, attribute, key=key),
                operand)

        else:  # subfield
            chain = op.split('.')
            field = chain[0]

            if (not isinstance(attribute.property, RelationshipProperty) or
                    not attribute.property.direction is sqlalchemy.orm.interfaces.MANYTOONE):
                raise ValueError("Can't get '{}' subfield from '{}': "
                                 "not a many-to-one relation".format(field,
                                                                     key))

            attr = getattr(attribute.property.mapper.class_, field)

            if isinstance(operand, dict):
                attr_criteria = operand
            else:
                attr_criteria = {'$eq': operand}

            if len(chain) > 1:
                for chain_attr in reversed(chain[1:]):
                    attr_criteria = {chain_attr: attr_criteria}

            target_class = attribute.property.mapper.class_
            attr_subquery = Query(target_class).filter(attribute.expression)
            expr = attr_subquery.filter(
                get_attribute_filter_expr(attr, attr_criteria,
                                          key=field)).exists()

        exprs.append(expr)

    return and_(*exprs)


def get_filter_expr(model, criteria):
    """
    :type model: sqlalchemy.orm.mapper.Mapper
    :type criteria: dict
    """
    exprs = []
    for op, operand in criteria.items():
        if op in primitive_operators or op in relationship_operators:
            raise ValueError(
                "Can't use operator '{}' on the top level".format(op))

        if op in logical_operators:
            expr = logical_operators[op](partial(get_filter_expr, model),
                                         operand)

        else:  # attribute
            chain = op.split('.')

            attr = getattr(model, chain[0])

            if isinstance(operand, dict):
                attr_criteria = operand
            else:
                attr_criteria = {'$eq': operand}

            if len(chain) > 1:
                for chain_attr in reversed(chain[1:]):
                    attr_criteria = {chain_attr: attr_criteria}

            expr = get_attribute_filter_expr(attr, attr_criteria, key=chain[0])

        exprs.append(expr)

    return and_(*exprs)


def get_direction(order_by):
    if order_by[0] == '-':
        return order_by[1:], -1
    elif order_by[0] == '+':
        return order_by[1:], 1
    else:
        return order_by, 1


def get_order_by(model, order_by):
    # entity = get_query_entities(q)[0]

    if not isinstance(order_by, list):
        order_by = [order_by]

    orderings = []
    joins = []

    for item in order_by:
        field_chain, direction = get_direction(item)
        fields = field_chain.split('.')
        field = fields[0]

        attr = getattr(model, field)

        for i, field in enumerate(fields[1:]):
            if (not isinstance(attr.property, RelationshipProperty) or
                    not attr.property.direction is sqlalchemy.orm.interfaces.MANYTOONE):
                raise ValueError("'{}' is not a many-to-one relation".format(
                    '.'.join(fields[:i + 1])))

            alias = aliased(attr.property.mapper.class_)
            joins.append((alias, attr))
            attr = getattr(alias, field)

        if not isinstance(attr.property, ColumnProperty):
            raise ValueError("'{}' is not a column".format(field_chain))

        if direction > 0:
            orderings.append(attr.asc())
        else:
            orderings.append(attr.desc())

    return orderings, joins


def apply_order_by(query, orderings, joins):
    """
    :type query: sqlalchemy.orm.query.Query
    :type orderings: list of sqlalchemy.sql.elements.UnaryExpression
    :rtype: sqlalchemy.orm.query.Query
    """
    for alias, attr in joins:
        query = query.outerjoin(alias, attr)

    return make_order_by_deterministic(query.order_by(*orderings))

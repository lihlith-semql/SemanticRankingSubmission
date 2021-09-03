
from typing import Set

from dataclasses import dataclass

from semql.core.ast import *

from semql.to_text.util import get_table_name, get_attr_name


@dataclass
class OpTypeError:
    message: str


@dataclass
class UnionType:
    member_types: Set


class EmptyType:
    pass


EMPTY_TYPE = EmptyType()


def type_iter(t):
    if isinstance(t, str):
        yield t
    elif isinstance(t, UnionType):
        yield from t.member_types
    elif isinstance(t, EmptyType):
        yield EMPTY_TYPE
    elif isinstance(t, OpTypeError):
        yield t
    else:
        raise ValueError(f"unknown (internal) type with value {t} of (python) type {type(t)}")


def check_missing(op: Operation, db_meta: dict, merge_keys):
    provided = set()
    required = set()
    for leaf in op.return_leaves():
        table_name = leaf.table_name
        table = db_meta['tables'][table_name]
        required.update(table.components(merge_keys))
        provided.add(table_name)

    missing = required - provided
    return missing


def all_merge_keys(op: Operation):
    if isinstance(op, Merge):
        tab0, attr0 = get_table_name(op.attribute_name0), get_attr_name(op.attribute_name0)
        tab1, attr1 = get_table_name(op.attribute_name1), get_attr_name(op.attribute_name1)
        local = {
            tab0: {attr0},
            tab1: {attr1},
        }
    else:
        local = {}

    child_merges = [all_merge_keys(child) for child in op.children]

    tables = set(local.keys()).union(
        k for c in child_merges for k in c.keys()
    )

    return {
        tab: local.get(tab, set()).union(
            attr for c in child_merges for attr in c.get(tab, set())
        )
        for tab in tables
    }


def compatible(concrete_type, other_type, op):
    assert isinstance(concrete_type, str)

    if isinstance(other_type, str):
        if concrete_type == other_type:
            return concrete_type
        else:
            return OpTypeError(message=f"incompatible types {concrete_type} and {other_type} in {op.print()}")
    elif isinstance(other_type, UnionType):

        check_subtypes = [
            compatible(concrete_type, member, op)
            for member in other_type.member_types
        ]
        no_typeerrs = [
            t
            for t in check_subtypes
            if not isinstance(t, OpTypeError)
        ]

        if len(no_typeerrs) > 0:
            return other_type
        else:
            return OpTypeError(message=f"incompatible types {concrete_type} and {other_type} in {op.print()}")
    elif isinstance(other_type, OpTypeError):
        return other_type
    else:
        raise ValueError(f"unknown {other_type} of type {type(other_type)}")


def type_check(op: Operation, db_meta_data: Dict):

    if isinstance(op, GetData):
        if db_meta_data['tables'].get(op.table_name) is None:
            return OpTypeError(message=f"table '{op.table_name}' does not exist")
        else:
            return op.table_name
    elif any(isinstance(op, op_type) for op_type in {Filter, Distinct, ExtractValues, Min, Max}):
        expected_type = get_table_name(op.attribute_name)
        child_type = type_check(op.children[0], db_meta_data)
        return compatible(expected_type, child_type, op)
    elif any(isinstance(op, op_type) for op_type in {Sum, Average}):
        expected_type = get_table_name(op.attribute_name)
        child_type = type_check(op.children[0], db_meta_data)
        return_type = compatible(expected_type, child_type, op)
        if isinstance(return_type, OpTypeError):
            return return_type
        else:
            return EMPTY_TYPE
    elif any(isinstance(op, op_type) for op_type in {Done, Count, IsEmpty}):
        child_type = type_check(op.children[0], db_meta_data)
        if isinstance(child_type, OpTypeError):
            return child_type
        else:
            return EMPTY_TYPE
    elif isinstance(op, Merge):
        child_types = {
            tpe
            for child in op.children
            for tpe in type_iter(type_check(child, db_meta_data))
        }
        errors = {
            child_type
            for child_type in child_types
            if isinstance(child_type, OpTypeError)
        }
        if len(errors) > 0:
            return OpTypeError(message="\n".join(err.message for err in errors))
        else:
            return UnionType(member_types=child_types)
    elif isinstance(op, Union):
        raise NotImplementedError(f"todo: {op.__class__.__name__}")
    elif isinstance(op, Difference):
        raise NotImplementedError(f"todo: {op.__class__.__name__}")
    elif isinstance(op, Intersection):
        raise NotImplementedError(f"todo: {op.__class__.__name__}")
    elif isinstance(op, SumBy):
        raise NotImplementedError(f"todo: {op.__class__.__name__}")
    elif isinstance(op, AverageBy):
        raise NotImplementedError(f"todo: {op.__class__.__name__}")
    elif isinstance(op, CountBy):
        raise NotImplementedError(f"todo: {op.__class__.__name__}")
    else:
        raise NotImplementedError(f"type checking for {op.__class__.__name__}")

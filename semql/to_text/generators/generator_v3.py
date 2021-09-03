
from typing import Optional
import copy
import re
from collections import Counter

from semql.to_text.typed_ops import *
from semql.to_text.config import UNSUPPORTED_OPS
from semql.to_text.util import (
    get_attr, get_table_name, get_attr_name, iter_nodes_by_predicate)
from semql.to_text.token import (
    replace_first, replace_all, Token, AttributeToken, TableToken,
    TokenContentMatcher, TokenWrap)
from semql.to_text.inflect import plural_noun


def _clone_ctx(ctx: Dict):
    if ctx is None:
        return {}
    else:
        return copy.copy(ctx)


def _clone_leaf_data(leaf_data: Dict):
    return _clone_ctx(leaf_data)


def generator_v3(op: Operation, db_meta_data: Dict, string_only: bool = True):
    tokens, leaves = GeneratorV3(db_meta_data)(op, ctx=None)

    merge_keys = all_merge_keys(op)
    for m in check_missing(op, db_meta_data, merge_keys):
        missing_tab = db_meta_data['tables'][m]
        leaves[m] = [Token(plural_noun(missing_tab.pretty_name), op_ref=NoOp())]

    res = tokens
    for entity_name, entity_str in leaves.items():

        # replace first mention with full entity description
        res = replace_first(
            tokens=res,
            token_matcher=TokenContentMatcher(f"${entity_name}"),
            to_insert=entity_str,
        )

        # subsequent mentions should be relative
        pretty_name = db_meta_data['tables'][entity_name].pretty_name
        those = Token("those", op_ref=NoOp())
        ent = TableToken(
            plural_noun(pretty_name), op_ref=NoOp(), table_name=entity_name)
        res = replace_all(
            tokens=res,
            token_matcher=TokenContentMatcher(f"${entity_name}"),
            to_insert=[those, ent],
        )

    if string_only:
        res = ' '.join(t.content for t in res)  # will have too many whitespaces
        res = re.sub(r'\s([?!.,](?:\s|$))', r'\1', res)  # remove whitespace in front of punctuation

    return res


class GeneratorV3:

    def __init__(self, db_meta_data: Dict):
        self.db_meta_data = db_meta_data
        self.basic_root_gen = RootNodeGenerator(
            parent_generator=self,
            db_meta_data=self.db_meta_data
        )
        self.agg_root_gen = AggregateRootGenerator(
            parent_generator=self,
            db_meta_data=self.db_meta_data,
        )
        self.min_max_gen = MinMaxGenerator(
            parent_generator=self,
            db_meta_data=self.db_meta_data,
        )
        self.proj_root_gen = ProjRootGen(
            parent_gen=self,
            db_meta_data=self.db_meta_data,
        )

    def __call__(self, op: Operation, ctx=None):
        if any(isinstance(op, unsupported) for unsupported in UNSUPPORTED_OPS):
            raise ValueError(
                f"cannot generate text for unsupported operation: {op.print_node()}")

        if any(
                isinstance(op, clz)
                for clz in self.basic_root_gen.supported_operations()
        ):
            # setup global context
            merge_keys = all_merge_keys(op)
            ctx = {
                'missing': check_missing(op, self.db_meta_data, merge_keys),
                'merge_keys': merge_keys,
            }
            return self.basic_root_gen(op, ctx=ctx)
        elif any(
            isinstance(op, clz)
            for clz in self.agg_root_gen.supported_operations()
        ):
            # setup global context
            merge_keys = all_merge_keys(op)
            ctx = {
                'missing': check_missing(op, self.db_meta_data, merge_keys),
                'merge_keys': merge_keys,
            }
            return self.agg_root_gen(op, ctx=ctx)
        elif any(
                isinstance(op, clz)
                for clz in self.proj_root_gen.supported_operations()
        ):
            # setup global context
            merge_keys = all_merge_keys(op)
            ctx = {
                'missing': check_missing(op, self.db_meta_data, merge_keys),
                'merge_keys': merge_keys,
            }
            return self.proj_root_gen(op, ctx=ctx)
        elif isinstance(op, ExtractValues):
            attr = get_attr(op.attribute_name, self.db_meta_data).name
            ctx = _clone_ctx(ctx)
            ctx['main_query_table'] = get_table_name(op.attribute_name)
            sub, leaf_data = self(op.children[0], ctx)

            attr = AttributeToken(
                content=plural_noun(attr),
                op_ref=op,
                attribute_name=op.attribute_name,
            )
            of = Token(content="of", op_ref=op)

            res = [attr, of] + sub

            distinct = find_distinct(op)
            if distinct is not None:
                if distinct.attribute_name != op.attribute_name:
                    raise ValueError(f"attribute of 'Distinct' operation"
                                     f"expected to match attribute of 'ExtractValues'"
                                     f" operation"
                                     f"found {distinct.print_node()} in tree with"
                                     f" {op.print_node()}")
                res = [Token(content="distinct", op_ref=distinct)] + res


            return res, leaf_data
        elif isinstance(op, Distinct):
            return self(op.children[0], ctx)
        elif any(
            isinstance(op, clz)
            for clz in self.min_max_gen.supported_operations()
        ):
            return self.min_max_gen(op, ctx)
        elif isinstance(op, Filter):
            ctx = _clone_ctx(ctx)

            if ctx.get('filters') is None:
                ctx['filters'] = {}

            table = get_table_name(op.attribute_name)

            if ctx['filters'].get(table) is None:
                ctx['filters'][table] = []

            operation = op.operation
            if operation.lower() == 'like':
                operation = '='

            ctx['filters'][table].append({
                'attr_key': get_attr_name(op.attribute_name),
                'attribute': get_attr(op.attribute_name, self.db_meta_data),
                'comparator': operation,
                'literal': op.value,
                'node_ref': op,
            })

            return self(op.children[0], ctx)
        elif isinstance(op, GetData):
            table = self.db_meta_data['tables'][op.table_name]
            return table.text(ctx, op)
        elif isinstance(op, Merge):
            return _merge_gen(op, ctx, self, self.db_meta_data)
        else:
            raise ValueError(
                f"cannot generate text for unknown operation: {op.print_node()}")


def _merge_gen(op, ctx, parent_gen, db_meta):

    left, right = op.children[0], op.children[1]
    # 'merge_keys' is set globally when evaluating root and never modified -> no need to copy
    left_type = _branch_fn_type(left, db_meta, ctx['merge_keys'])
    right_type = _branch_fn_type(right, db_meta, ctx['merge_keys'])

    left_has_main = ctx['main_query_table'] in left_type['out']
    right_has_main = ctx['main_query_table'] in right_type['out']

    left_is_entity = _check_entity_branch(left, db_meta, ctx['merge_keys'])
    left_entity_type = type_check(left, db_meta)
    right_is_entity = _check_entity_branch(right, db_meta, ctx['merge_keys'])
    right_entity_type = type_check(right, db_meta)

    assert not (left_is_entity and right_is_entity)  # can't merge entities only entity and relation

    left_ctx = _clone_ctx(ctx)
    right_ctx = _clone_ctx(ctx)
    if left_has_main and right_has_main:
        merge_on = ctx['main_query_table']
    elif left_has_main and not right_has_main:
        merge_on = list(set(left_type['out']) & set(right_type['out']))[0]
        right_ctx['main_query_table'] = merge_on
    elif right_has_main and not left_has_main:
        merge_on = list(set(left_type['out']) & set(right_type['out']))[0]
        left_ctx['main_query_table'] = merge_on
    else:
        raise ValueError("should have been caught by earlier assertion")

    left_out, left_leaves = parent_gen(left, left_ctx)
    right_out, right_leaves = parent_gen(right, right_ctx)
    out_leaves = _merge_out(left_leaves, right_leaves)

    if left_has_main and right_has_main:
        # both are compound / relations
        src, tgt = (left_out, right_out) \
            if len(left_type['out']) >= len(right_type['out'])\
            else (right_out, left_out)
    elif left_has_main and not right_has_main:
        src = left_out
        tgt = right_out
    elif right_has_main and not left_has_main:
        src = right_out
        tgt = left_out
    else:
        raise ValueError("should have been caught by earlier assertion")

    result = replace_first(
        tokens=src,
        token_matcher=TokenContentMatcher(f"${merge_on}"),
        to_insert=tgt,
    )

    return result, out_leaves


def _merge_out(left_leaves: dict, right_leaves: dict):
    assert len(set(left_leaves.keys()) & set(right_leaves.keys())) == 0
    return {
        k: v
        for s in [left_leaves, right_leaves]
        for k, v in s.items()
    }


def _check_entity_branch(op: Operation, db_meta: dict, merge_keys: dict):
    tpe = type_check(op, db_meta)

    if isinstance(tpe, UnionType):
        return False
    elif isinstance(tpe, str):
        table = db_meta['tables'].get(tpe)
        if table.is_relation(merge_keys):
            return False
        else:
            return True
    else:
        raise ValueError(f"tree broken")


def _branch_fn_type(branch_op, db_meta, merge_keys):
    branch_type = type_check(branch_op, db_meta)

    if isinstance(branch_type, UnionType):
        count_outs = Counter(
            o
            for member in branch_type.member_types
            for o in db_meta['tables'][member].fn_type(merge_keys)['out']
        )
        count_ins = Counter(
            i
            for member in branch_type.member_types
            for i in db_meta['tables'][member].fn_type(merge_keys)['in']
        )
        return {
            'in': sorted([k for k in count_ins.keys() if (count_ins[k] - count_outs[k]) >= 0]),
            'out': sorted(count_outs.keys()),
        }
    elif isinstance(branch_type, str):
        table = db_meta['tables'][branch_type]
        return table.fn_type(merge_keys)
    else:
        raise ValueError(f"expected table or union type found "
                         f"{branch_type} of type {type(branch_type)}")


class MinMaxGenerator:

    def __init__(self, parent_generator, db_meta_data):
        self.templates = {
            "Min": ["$sub", "with minimum", "$attr"],
            "Max": ["$sub", "with maximum", "$attr"],
        }
        self.parent = parent_generator
        self.db_meta = db_meta_data

    def __call__(self, op, ctx=None):
        template = self.templates.get(op.__class__.__name__)

        if template is None:
            raise ValueError(
                f"can only handle nodes: "
                f"{[op.__name__ for op in self.supported_operations()]}")

        attr = get_attr(op.attribute_name, self.db_meta).name
        sub, leaf_data = self.parent(op.children[0], ctx)

        wrapper = TokenWrap(op)

        result = replace_first(
            tokens=wrapper(template),
            token_matcher=TokenContentMatcher("$attr"),
            to_insert=[AttributeToken(attr, op, op.attribute_name)],
        )

        result = replace_first(
            tokens=result,
            token_matcher=TokenContentMatcher("$sub"),
            to_insert=sub,
        )

        return result, leaf_data

    @staticmethod
    def supported_operations():
        return [Min, Max]


class AggregateRootGenerator:

    def __init__(self, db_meta_data, parent_generator):
        self.templates = {
            "Sum": [
                "What is the total", "$attr", "of all", "$sub", "?"],
            "Average": [
                "What is the average", "$attr", "of all", "$sub", "?"],
            "MinAggregation": [
                "What is the minimum", "$attr", "of all", "$sub", "?"],
            "MaxAggregation": [
                "What is the maximum", "$attr", "of all", "$sub", "?"],
        }
        self.db_meta = db_meta_data
        self.parent = parent_generator

    def __call__(self, op, ctx=None):
        template = self.templates.get(op.__class__.__name__)

        if template is None:
            raise ValueError(
                f"can only handle root nodes: "
                f"{[op.__class__.__name__ for op in self.supported_operations()]}")

        attr = get_attr(op.attribute_name, self.db_meta).name
        if ctx is None:
            ctx = {}
        else:
            ctx = _clone_ctx(ctx)
        ctx['main_query_table'] = get_table_name(op.attribute_name)
        sub, leaf_data = self.parent(op.children[0], ctx)

        distinct = find_distinct(op)
        if distinct is not None:
            table = self.db_meta['tables'][get_table_name(op.attribute_name)]
            if distinct.attribute_name != table.primary_key():
                raise ValueError(
                    f"attribute of 'Distinct' Operation in a tree of type "
                    f"'Average' or 'Sum' is expected to be PrimaryKey"
                    f"found {distinct.print_node()} in {op.print_node()}"
                )
            sub = [Token("distinct", op_ref=distinct)] + sub


        wrapper = TokenWrap(op)

        result = replace_first(
            tokens=wrapper(template),
            token_matcher=TokenContentMatcher("$attr"),
            to_insert=[AttributeToken(
                content=attr,
                op_ref=op,
                attribute_name=op.attribute_name,
            )],
        )

        result = replace_first(
            tokens=result,
            token_matcher=TokenContentMatcher("$sub"),
            to_insert=sub,
        )

        return result, leaf_data

    @staticmethod
    def supported_operations() -> List[Operation]:
        return [Sum, Average, MinAggregation, MaxAggregation]


class RootNodeGenerator:

    def __init__(self, db_meta_data, parent_generator):
        self.templates = {
            "Done": ["What are the", "$child", "?"],
            "IsEmpty": ["Are there any", "$child", "?"],
            "Count": ["How many",  "$child", "are there", "?"],  # dont attach ? here so patch in generator works
        }
        self.special_template = ["Show me everything about", "$child", "."]
        self.db_meta = db_meta_data
        self.parent = parent_generator

    def __call__(self, base_node: Operation, ctx=None):
        template = self.templates.get(base_node.__class__.__name__)

        if template is None:
            raise ValueError(
                f"can only handle root nodes: "
                f"{[op.__class__.__name__ for op in self.supported_operations()]}")

        if ctx is None:
            ctx = {}
        else:
            ctx = _clone_ctx(ctx)

        if not contains_projection(base_node):
            if isinstance(base_node, Done):
                template = self.special_template

            merge_keys = ctx.get('merge_keys')
            if merge_keys is None:
                merge_keys = all_merge_keys(base_node)
                ctx['merge_keys'] = merge_keys

            leafs = [
                lf
                for lf in base_node.return_leaves()
                if not self.db_meta['tables'][lf.table_name].is_relation(merge_keys)
            ]

            ctx['main_query_table'] = leafs[0].table_name

        child_query, leave_data = self.parent(base_node.children[0], ctx=ctx)

        wrapper = TokenWrap(node_ref=base_node)

        return replace_first(
            tokens=wrapper(template),
            token_matcher=TokenContentMatcher("$child"),
            to_insert=child_query,
        ), leave_data

    @staticmethod
    def supported_operations() -> List[Operation]:
        return [Done, IsEmpty, Count]


class ProjRootGen:

    def __init__(self, db_meta_data, parent_gen):
        self.db_meta = db_meta_data
        self.parent = parent_gen

        self.main_template = ["What are the", "$attr_list", "of", "$child", "?"]
        self.additional = ["Also show the", "$attr_list", "of the", "$table", "."]

    def __call__(self, base_node: Operation, ctx: dict):
        assert isinstance(base_node, ProjectionRoot)

        table_count = Counter(
            get_table_name(attr)
            for attr, _ in base_node.attrs
        )
        attrs_per_table = {
            k: []
            for k in table_count.keys()
        }
        for attr_name, fn in base_node.attrs:
            attrs_per_table[get_table_name(attr_name)].append((attr_name, fn))
        ordered_tables = sorted(
            table_count.keys(), key=lambda t: table_count.get(t), reverse=True)

        ctx = _clone_ctx(ctx)
        main_tab = ordered_tables[0]
        ctx['main_query_table'] = main_tab
        child_query, leaf_data = self.parent(base_node.children[0], ctx=ctx)

        out = self.__build_main_query(
            attributes=attrs_per_table[main_tab],
            base_node=base_node,
            child_query=child_query,
        )

        for add_tab in ordered_tables[1:]:
            out += self.__build_additional(
                table_name=add_tab,
                attributes=attrs_per_table[add_tab],
                base_node=base_node,
            )

        return out, leaf_data

    def __build_main_query(
            self,
            attributes: List[Tuple[str, ProjectionRoot.ProjectionFN]],
            base_node: Operation,
            child_query: List[Token]
    ) -> List[Token]:
        attr_tokens = [
            self.__render_attr(a_str, fn, base_node)
            for a_str, fn in attributes
        ]
        attr_list = ProjRootGen.__enumerate_attrs(attr_tokens)
        wrapper = TokenWrap(node_ref=base_node)

        res = replace_first(
            tokens=wrapper(self.main_template),
            token_matcher=TokenContentMatcher("$attr_list"),
            to_insert=attr_list,
        )

        res = replace_first(
            tokens=res,
            token_matcher=TokenContentMatcher("$child"),
            to_insert=child_query,
        )

        return res

    def __build_additional(
            self,
            table_name: str,
            attributes: List[Tuple[str, ProjectionRoot.ProjectionFN]],
            base_node: Operation,
    ):
        attr_tokens = [
            self.__render_attr(a_str, fn, base_node)
            for a_str, fn in attributes
        ]
        attr_list = ProjRootGen.__enumerate_attrs(attr_tokens)

        wrapper = TokenWrap(node_ref=base_node)
        res = replace_first(
            tokens=wrapper(self.additional),
            token_matcher=TokenContentMatcher("$attr_list"),
            to_insert=attr_list,
        )

        pretty_name = self.db_meta['tables'][table_name].pretty_name
        table_token = TableToken(
            content=plural_noun(pretty_name),
            op_ref=NoOp(),
            table_name=table_name,
        )

        res = replace_first(
            tokens=res,
            token_matcher=TokenContentMatcher("$table"),
            to_insert=[table_token],
        )

        return res

    def __render_attr(
            self,
            attr_str: str,
            proj_fn: ProjectionRoot.ProjectionFN,
            base_node: Operation,
    ) -> AttributeToken:

        if attr_str == "*.*":
            assert proj_fn == ProjectionRoot.ProjectionFN.COUNT,\
                "only COUNT fn should have a '*' argument in ProjectionRoot"
            return AttributeToken(
                content="number of entries",
                op_ref=base_node,
                attribute_name=attr_str,
            )

        pretty_attr_name = get_attr(
            attr_str=attr_str, db_meta_data=self.db_meta).name

        if proj_fn == ProjectionRoot.ProjectionFN.NONE:
            text = plural_noun(pretty_attr_name)
        elif proj_fn == ProjectionRoot.ProjectionFN.SUM:
            text = f"total {pretty_attr_name}"
        elif proj_fn == ProjectionRoot.ProjectionFN.AVG:
            text = f"average {pretty_attr_name}"
        elif proj_fn == ProjectionRoot.ProjectionFN.MIN:
            text = f"minimum {pretty_attr_name}"
        elif proj_fn == ProjectionRoot.ProjectionFN.MAX:
            text = f"maximum {pretty_attr_name}"
        elif proj_fn == ProjectionRoot.ProjectionFN.COUNT:
            text = f"number of {plural_noun(pretty_attr_name)}"
        else:
            raise ValueError(f"unknown aggreagation fn {proj_fn}")

        return AttributeToken(
            content=text,
            op_ref=base_node,
            attribute_name=attr_str,
        )

    @staticmethod
    def __enumerate_attrs(attrs: List[AttributeToken]) -> AttributeToken:

        if len(attrs) < 2:
            return attrs
        elif len(attrs) == 2:
            and_token = Token(content="and", op_ref=attrs[0].op_ref)
            return [attrs[0], and_token, attrs[1]]
        else:
            res = []
            for at in attrs[:-1]:
                comma = Token(content=',', op_ref=at.op_ref)
                res.append(at)
                res.append(comma)

            last = attrs[-1]
            and_token = Token(content='and', op_ref=last.op_ref)
            res.append(and_token)
            res.append(last)

            return res

    @staticmethod
    def supported_operations() -> List[Operation]:
        return [ProjectionRoot]


def contains_projection(tree: Operation) -> bool:
    projs = iter_nodes_by_predicate(
        tree,
        predicate=lambda t: isinstance(t, ExtractValues)
    )
    return len(list(projs)) > 0


def find_distinct(tree: Operation) -> Optional[Distinct]:
    distincts = list(iter_nodes_by_predicate(
        tree, predicate=lambda t: isinstance(t, Distinct)))

    if len(distincts) == 0:
        return None
    elif len(distincts) == 1:
        return distincts[0]
    else:
        raise ValueError(
            f"cannot handle more than one 'distinct' operation in tree {tree}")


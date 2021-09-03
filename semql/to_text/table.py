
from typing import Dict, List, Set, Optional
from functools import reduce
import copy

from semql.core.ast import GetData
from semql.to_text.inflect import plural_noun
from semql.to_text.attribute import PrimaryKey
from semql.to_text.token import (
    replace_first, Token, TableToken, TokenWrap, TokenContentMatcher)


def combine_filters(filter_tokens: List[List[Token]]) -> List[Token]:
    if len(filter_tokens) == 0:
        return []
    elif len(filter_tokens) == 1:
        return filter_tokens[0]
    else:
        def concat(l, r):
            comb = Token(
                content="and",
                op_ref=l[0].op_ref,
            )
            return l + [comb] + r
        return reduce(lambda l, r: concat(l=l, r=r), filter_tokens)


class Table:

    def __init__(
            self,
            name,
            attributes,
            pretty_name=None,
    ):
        self.name = name
        self.attributes = attributes
        if pretty_name is None:
            self.pretty_name = self.name.lower()
        else:
            self.pretty_name = pretty_name

        self.__pkey = self.__primary_key()
        self.__fkeys = self.__foreign_keys()
        self.__default = self.__default_attr()
        self.__fkey_m = self.__fkey_map()

    def is_relation(self, merge_keys):
        return False

    def primary_key(self) -> Optional[str]:
        return copy.copy(self.__pkey)

    def foreign_keys(self) -> Set[str]:
        return copy.copy(self.__fkeys)

    def foreign_key_map(self) -> Dict[str, str]:
        return copy.copy(self.__fkey_m)

    def default_attribute(self) -> Optional[str]:
        return copy.copy(self.__default)

    def __fkey_map(self):
        return {
            attr.target_table: f"{self.name}.{aname}"
            for aname, attr in self.attributes.items()
            if attr.is_foreign_key()
        }

    def __primary_key(self):
        pkeys = [
            aname
            for aname, attr in self.attributes.items()
            if attr.is_primary_key()
        ]
        if len(pkeys) == 0:
            return None
        elif len(pkeys) == 1:
            return f"{self.name}.{pkeys[0]}"
        else:
            raise ValueError(
                f"Table {self.name} has more than one PrimaryKey attribute")

    def __foreign_keys(self):
        return {
            f"{self.name}.{aname}"
            for aname, attr in self.attributes.items()
            if attr.is_foreign_key()
        }

    def __default_attr(self):
        candidates = [
            f"{self.name}.{aname}"
            for aname, attr in self.attributes.items()
            if attr.is_default()
        ]
        if len(candidates) == 0:
            return None
        elif len(candidates) == 1:
            return candidates[0]
        else:
            raise ValueError(
                f"Table {self.name} has mor then one default attribute")

    def components(self, merge_keys):
        return {self.name}

    def text(self, query_context, node: GetData):
        filters = query_context.get('filters', {}).get(self.name, [])

        filter_tokens = [
            filter_data['attribute'].filter(
                comparator=filter_data['comparator'],
                literal=filter_data['literal'],
                node_ref=filter_data['node_ref'],
            )
            for filter_data in reversed(filters)
        ]
        filter_tokens = combine_filters(filter_tokens)

        entity_tokens = [
            TableToken(
                content=plural_noun(self.pretty_name),
                op_ref=node,
                table_name=node.table_name,
            )
        ]

        if len(filter_tokens) > 0:
            entity_tokens += filter_tokens

        tab_tok = TableToken(
            content=f"${self.name}",
            op_ref=node,
            table_name=node.table_name,
        )

        return [tab_tok], {self.name: entity_tokens}

    def fn_type(self, merge_keys):
        return {
            'in': [],
            'out': [self.name],
        }


class RelationTable(Table):

    def __init__(
            self,
            name,
            attributes,
            relation_components,
            templates,
            attr_templates=None,
            pretty_name=None,
    ):
        super(RelationTable, self).__init__(name, attributes, pretty_name)
        self.relation_components = relation_components
        self.templates = templates
        self.attr_templates = attr_templates

    def is_relation(self, merge_keys):
        return True

    def components(self, merge_keys):
        return copy.copy(self.relation_components)

    def text(self, query_context, node: GetData):
        queried_table = query_context['main_query_table']

        wrapper = TokenWrap(node_ref=node)

        if queried_table not in self.relation_components:
            raise ValueError(
                f"relation table {self.name} queried for {queried_table}")

        if self.attr_templates is None or len(self.attr_templates) == 0:
            return wrapper(self.templates[queried_table]), {}

        filters = query_context.get('filters', {}).get(self.name, [])

        filters_per_attr = {
            attr: []
            for attr in self.attr_templates.keys()
        }

        for filter_data in reversed(filters):
            filters_per_attr[filter_data['attr_key']].append(filter_data)

        filter_strings = {
            attr_key: combine_filters([
                filter_data['attribute'].filter(
                    comparator=filter_data['comparator'],
                    literal=filter_data['literal'],
                    node_ref=filter_data['node_ref'],
                )
                for filter_data in filter_ds]
            )
            for attr_key, filter_ds in filters_per_attr.items()
        }

        attr_filters = {
            attr_key: replace_first(
                tokens=wrapper(self.attr_templates[attr_key]),
                token_matcher=TokenContentMatcher(content="$filter"),
                to_insert=filter_string,
            ) if len(filter_string) > 0 else []
            for attr_key, filter_string in filter_strings.items()
        }

        result_tokens = wrapper(self.templates[queried_table])
        for attr_key, attr_tokens in attr_filters.items():
            result_tokens = replace_first(
                tokens=result_tokens,
                token_matcher=TokenContentMatcher(content=f"${attr_key}"),
                to_insert=attr_tokens,
            )

        return result_tokens, {}

    def fn_type(self, merge_keys):
        return {
            'in': sorted(self.relation_components),
            'out': sorted(self.relation_components),
        }


class PartialRelation(Table):

    def __init__(
            self,
            name,
            attributes,
            templates,
            pretty_name=None,
    ):
        super().__init__(name, attributes, pretty_name)
        self.templates = templates

    def _active_templates(self, merge_keys):
        merge_attrs = merge_keys.get(self.name, set())
        return merge_attrs.intersection(self.templates.keys())

    def components(self, merge_keys):
        if not self.is_relation(merge_keys):
            return {self.name}
        else:
            return {
                ent
                for tk in self._active_templates(merge_keys)
                for ent in self.templates[tk].keys()
            }

    def is_relation(self, merge_keys):
        return len(self._active_templates(merge_keys)) > 0

    def text(self, query_context, node: GetData):
        self_tokens, leaf_data = super().text(query_context, node)

        wrapper = TokenWrap(node_ref=node)

        main_ent = query_context['main_query_table']
        for foreign_key in self._active_templates(query_context['merge_keys']):
            templates = self.templates[foreign_key]
            template = templates.get(main_ent, templates[self.name])
            self_tokens = replace_first(
                tokens=wrapper(template),
                token_matcher=TokenContentMatcher(content=f"${self.name}"),
                to_insert=self_tokens,
            )

        return self_tokens, leaf_data

    def fn_type(self, merge_keys):
        components = self.components(merge_keys)
        return {
            'in': {e for e in components if e != self.name},
            'out': components,
        }

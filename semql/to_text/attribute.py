
from typing import List

from semql.core.ast import Filter
from semql.to_text.inflect import plural_noun
from semql.to_text.token import (
    Token, ComparatorToken, AttributeToken, LiteralToken, TokenWrap)


OPERATIONS = ["=", '==', '<', '>', '>=', '<=', '!=']


class Attribute:

    def __init__(self, attr_type, name, is_default: bool = False, **kw):
        self.type = attr_type
        self.name = name
        self.__default = is_default

    def is_primary_key(self):
        return False

    def is_foreign_key(self):
        return False

    def is_default(self):
        return self.__default

    def is_summable(self):
        return False

    def is_comparable(self):
        return False

    def comparators(self):
        return {
            '=': "is",
            "!=": "is not",
        }

    def filter(self, comparator, literal, node_ref: Filter) -> List[Token]:
        if comparator in {'=', '=='}:
            comp_tokens = ["is"]
        elif comparator == '!=':
            comp_tokens = ["is", "not"]
        else:
            raise ValueError(f"comparison type {comparator} not implemented "
                             f"for attributes of type {self.__class__.__name__}")

        comp_tokens = [
            ComparatorToken(
                content=t,
                op_ref=node_ref,
                attribute_name=node_ref.attribute_name,
            )
            for t in comp_tokens
        ]

        name = AttributeToken(
            content=self.name,
            op_ref=node_ref,
            attribute_name=node_ref.attribute_name,
        )

        lit = LiteralToken(
            content=str(literal),
            op_ref=node_ref,
            attribute_name=node_ref.attribute_name,
        )

        return [Token(
            content="whose", op_ref=node_ref), name] + comp_tokens + [lit]


class PrimaryKey(Attribute):

    def __init__(self):
        super(PrimaryKey, self).__init__(attr_type="primary_key", name="id")

    def is_primary_key(self):
        return True


class ForeignKey(Attribute):

    def __init__(self, name, target_table: str ,**kw):
        super(ForeignKey, self).__init__(attr_type='foreign_key', name=name,**kw)
        self.target_table = target_table

    def is_foreign_key(self):
        return True


class VerbPhrase(Attribute):

    def __init__(self, name, auxiliary, participle, preposition, is_default=False, **kw):
        super(VerbPhrase, self).__init__(attr_type="verb_phrase", name=name, is_default=is_default, **kw)
        self.aux = auxiliary
        self.participle = participle
        self.prep = preposition

    def comparators(self):
        return {
            "=": f"who {self.aux} {self.participle} {self.prep}",
            "!=": f"who {self.aux} not {self.participle} {self.prep}",
        }

    def filter(self, comparator, literal, node_ref: Filter):
        if comparator in {'=', '=='}:
            comp_tokens = [" "]
        elif comparator == '!=':
            comp_tokens = ["not"]
        else:
            raise ValueError(f"comparison type {comparator} not implemented "
                             f"for attributes of type {self.__class__.__name__}")

        comp_tokens = [
            ComparatorToken(
                content=t,
                op_ref=node_ref,
                attribute_name=node_ref.attribute_name,
            )
            for t in comp_tokens
        ]

        lit = LiteralToken(
            content=str(literal),
            op_ref=node_ref,
            attribute_name=node_ref.attribute_name,
        )

        attr = AttributeToken(
            self.participle, node_ref, node_ref.attribute_name)

        wrapper = TokenWrap(node_ref=node_ref)

        return wrapper(["who", self.aux]) + comp_tokens + [attr] + wrapper(
            [self.prep]) + [lit]


class Date(Attribute):

    def __init__(
            self,
            name,
            participle,
            preposition="on",
            auxiliary="were",
            is_default=False
    ):
        super(Date, self).__init__(
            attr_type="date", name=name, is_default=is_default)
        self.participle = participle
        self.preposition = preposition
        self.aux = auxiliary

    def comparators(self):
        return {
            "=": f"{self.preposition}",
            "!=": f"not ... {self.preposition}",
            "<": "before (not including)",
            "<=": "before (including)",
            ">": "after (not including)",
            ">=": "after (including)",
        }

    def is_comparable(self):
        return True

    def filter(self, comparator, literal, node_ref: Filter):
        if comparator in {'<', '<='}:
            comp_word = ['before']
        elif comparator in {'>', '>='}:
            comp_word = ['after']
        else:
            comp_word = [self.preposition]

        wrapper = TokenWrap(node_ref=node_ref)

        comp_word = [ComparatorToken(
            content=w,
            op_ref=node_ref,
            attribute_name=node_ref.attribute_name,
        ) for w in comp_word]

        neg = ComparatorToken(
            content="not",
            op_ref=node_ref,
            attribute_name=node_ref.attribute_name,
        )

        lit = LiteralToken(
            content=str(literal),
            op_ref=node_ref,
            attribute_name=node_ref.attribute_name,
        )

        return wrapper(["who", self.aux]) +\
               ([neg] if comparator == "!=" else []) +\
               [AttributeToken(
                   self.participle, node_ref, node_ref.attribute_name
               )] + comp_word + [lit]


class Location(VerbPhrase):

    def __init__(self, name, participle, is_default=False):
        super(Location, self).__init__(
            name=name,
            auxiliary="were",
            participle=participle,
            preposition="in",
            is_default=is_default,
        )


class Numeric(Attribute):

    def __init__(self, name, unit=None, is_default=False):
        super(Numeric, self).__init__(
            attr_type="numeric", name=name, is_default=is_default)
        self.unit = unit

    def is_comparable(self):
        return True

    def is_summable(self):
        return True

    def comparators(self):
        return {
            ">": "of more than",
            ">=": "of at least",
            "<": "of less than",
            "<=": "of at most",
            "=": "of",
            "!=": "other than",
        }

    def filter(self, comparator, literal, node_ref: Filter):
        if comparator == '>':
            comp_tokens = ["of", "more", "than"]
        elif comparator == '>=':
            comp_tokens = ["of", "at", "least"]
        elif comparator == '<':
            comp_tokens = ["of", "less", "than"]
        elif comparator == '<=':
            comp_tokens = ["of", "at", "most"]
        elif comparator in {'=', '=='}:
            comp_tokens = ["of"]
        elif comparator in {'!='}:
            comp_tokens = ["other", "than"]
        else:
            return super(Numeric, self).filter(comparator, literal, node_ref)

        wrapper = TokenWrap(node_ref)

        comp_tokens = [
            ComparatorToken(
                content=t,
                op_ref=node_ref,
                attribute_name=node_ref.attribute_name,
            )
            for t in comp_tokens
        ]

        name = AttributeToken(self.name, node_ref, node_ref.attribute_name)

        lit = LiteralToken(str(literal), node_ref, node_ref.attribute_name)

        return wrapper(["with a"]) + [name] + comp_tokens + [lit] +\
               wrapper([plural_noun(self.unit)] if self.unit else [" "])


class Enum(Attribute):

    def __init__(self, name, is_default=False):
        super(Enum, self).__init__(
            attr_type="enum", name=name, is_default=is_default)


class Name(Attribute):

    def __init__(self, is_default=False):
        super(Name, self).__init__(
            attr_type="name", name="name", is_default=is_default)


class Text(Attribute):

    def __init__(self, name, is_default=False):
        super(Text, self).__init__(
            attr_type="text", name=name, is_default=is_default)


class Literal(Attribute):

    def __init__(self, name, is_default=False):
        super(Literal, self).__init__(
            attr_type="literal", name=name, is_default=is_default)

    def filter(self, comparator, literal, node_ref: Filter):
        if comparator in {'=', '=='}:
            comp_tokens = [" "]
        elif comparator == '!=':
            comp_tokens = ["not"]
        else:
            raise ValueError(f"comparison type {comparator} not implemented "
                             f"for attributes of type {self.__class__.__name__}")

        comp_tokens = [ComparatorToken(
            content=t,
            op_ref=node_ref,
            attribute_name=node_ref.attribute_name,
        ) for t in comp_tokens]

        lit = LiteralToken(str(literal), node_ref, node_ref.attribute_name)

        return comp_tokens + [lit]

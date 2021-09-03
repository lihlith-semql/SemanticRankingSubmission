
from dataclasses import dataclass
from typing import List, Callable

from semql.core.ast import Operation


@dataclass
class Token:
    content: str
    op_ref: Operation


@dataclass
class AttributeToken(Token):
    attribute_name: str


@dataclass
class ComparatorToken(Token):
    attribute_name: str


@dataclass
class LiteralToken(Token):
    attribute_name: str


@dataclass
class TableToken(Token):
    table_name: str


class TokenWrap(Callable[[List[str]], List[Token]]):

    def __init__(self, node_ref: Operation):
        self.node_ref = node_ref

    def __call__(self, ts: List[str]) -> List[Token]:
        return [Token(content=t, op_ref=self.node_ref) for t in ts]


class TokenContentMatcher(Callable[[Token], bool]):

    def __init__(self, content: str):
        self.content = content

    def __call__(self, token: Token) -> bool:
        return token.content == self.content


def replace_first(
        tokens: List[Token],
        token_matcher: Callable[[Token], bool],
        to_insert: List[Token]
):
    match_mask = [token_matcher(t) for t in tokens]
    if any(match_mask):
        # find index of first token in list that matches
        first_ix = match_mask.index(True)
        return tokens[:first_ix] + to_insert + tokens[first_ix+1:]
    else:
        return tokens


def replace_all(
        tokens: List[Token],
        token_matcher: Callable[[Token], bool],
        to_insert: List[Token]
):
    match_mask = [token_matcher(t) for t in tokens]
    result = tokens
    while any(match_mask):
        result = replace_first(result, token_matcher, to_insert)
        match_mask = [token_matcher(t) for t in result]
    return result

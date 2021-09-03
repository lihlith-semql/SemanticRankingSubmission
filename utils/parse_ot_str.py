from semql.core.ast import *

P1 = 'op:attr'
P2 = 'op:attr:comp:val'
P3 = 'op:op:attr:attr'
P4 = 'tbl:db'
P5 = 'op'

patterns = {
    'sum': P1,
    'done': P5,
    'isEmpty': P5,
    'avg': P1,
    'distinct': P1,
    'count': P5,
    'extractValues': P1,
    'max': P1,
    'min': P1,
    'merge': P3,
    'filter': P2,
    'getData': P4,
}

def parenthetic_contents(string):
    """Generate parenthesized contents in string as pairs (level, contents)."""
    #https://stackoverflow.com/questions/4284991/parsing-nested-parentheses-in-python-grab-content-by-level
    stack = []
    for i, c in enumerate(string):
        if c == '(':
            stack.append(i)
        elif c == ')' and stack:
            start = stack.pop()
            yield (len(stack), string[start + 1: i])


def split_args(in_str, pattern):
    if pattern == P1:
        arg1 = in_str.split(',')[-1].strip()
        arg0 = in_str[:[m.start() for m in re.finditer(arg1, in_str)][-1] - 1]
        return arg0, arg1
    elif pattern == P2:
        inner_op_name = in_str.split('(')[0]
        #lvl_to_inner = {lvl: in_str for lvl, in_str in parenthetic_contents(in_str)}
        inner_args = [x[1] for x in parenthetic_contents(in_str) if x[0] == 0][0]
        arg0 = f'{inner_op_name}({inner_args})'
        rest = in_str.replace(f'{arg0},', '')
        arg1 = rest.split(',')[0].strip()
        arg2 = rest.split(',')[1].strip()
        arg3 = rest.replace(f'{arg1},{arg2},', '').strip()
        return arg0, arg1, arg2, arg3
    elif pattern == P3:
        arg3 = in_str.split(',')[-1].strip()
        arg2 = in_str.split(',')[-2].strip()
        rest = in_str[:[m.start() for m in re.finditer(f'{arg2},{arg3}', in_str)][-1] - 1]
        lvl_to_inner = [x[1] for x in parenthetic_contents(rest) if x[0] == 0][0]
        inner_op1_name = in_str.split('(')[0]
        arg0 = f'{inner_op1_name}({lvl_to_inner})'
        arg1 = rest[len(arg0) + 1:]
        return arg0, arg1, arg2, arg3
    elif pattern == P4:
        pass
    elif pattern == P5:
        pass
    else:
        pass


def translate_str_to_OT(ot_str: str,db:str) -> Operation:
    op_name = ot_str.split('(')[0]
    if op_name == 'NoOp':
        return NoOp()

    lvl_to_inner = {lvl: in_str for lvl, in_str in parenthetic_contents(ot_str)}
    args = lvl_to_inner[0]

    if op_name == 'done':
        child_op = translate_str_to_OT(args, db)
        return Done(result=child_op)
    elif op_name == 'sum':
        child_op_str, attr = split_args(args, patterns[op_name])
        child_op = translate_str_to_OT(child_op_str, db)
        return Sum(table=child_op, attribute_name=attr)
    elif op_name == 'avg':
        child_op_str, attr = split_args(args, patterns[op_name])
        child_op = translate_str_to_OT(child_op_str, db)
        return Average(table=child_op, attribute_name=attr)
    elif op_name == 'count':
        child_op = translate_str_to_OT(args, db)
        return Count(table=child_op)
    elif op_name == 'isEmpty':
        child_op = translate_str_to_OT(args, db)
        return IsEmpty(result=child_op)
    elif op_name == 'distinct':
        child_op_str, attr = split_args(args, patterns[op_name])
        child_op = translate_str_to_OT(child_op_str, db)
        return Distinct(result=child_op, attribute_name=attr)
    elif op_name == 'extractValues':
        child_op_str, attr = split_args(args, patterns[op_name])
        child_op = translate_str_to_OT(child_op_str, db)
        return ExtractValues(table=child_op, attribute_name=attr)
    elif op_name == 'max':
        child_op_str, attr = split_args(args, patterns[op_name])
        child_op = translate_str_to_OT(child_op_str, db)
        return Max(table=child_op, attribute_name=attr)
    elif op_name == 'min':
        child_op_str, attr = split_args(args, patterns[op_name])
        child_op = translate_str_to_OT(child_op_str, db)
        return Min(table=child_op, attribute_name=attr)
    elif op_name == 'merge':
        child_op_str1, child_op_str2, attr1, attr2 = split_args(args, patterns[op_name])
        child_op1 = translate_str_to_OT(child_op_str1, db)
        child_op2 = translate_str_to_OT(child_op_str2, db)
        return Merge(child_op1, child_op2, attr1, attr2)
    elif op_name == 'filter':
        child_op_str, attr, comp_op, val = split_args(args, patterns[op_name])
        child_op = translate_str_to_OT(child_op_str, db)
        return Filter(table=child_op, attribute_name=attr, operation=comp_op, value=val)
    elif op_name == 'getData':
        return GetData(table_name=args, data_source=db)
    else:
        return NoOp()


if __name__ == '__main__':
    in_str = 'count(extractValues(distinct(merge(filter(merge(getData(person),merge(getData(crew),merge(getData(movie),merge(getData(has_genre),getData(genre),has_genre.genre_id,genre.id),movie.id,has_genre.movie_id),crew.movie_id,movie.id),person.id,crew.person_id),person.birth_place,=,Bray, Berkshire, England),merge(merge(getData(has_genre),merge(getData(movie),getData(has_genre),movie.id,None),None,None),NoOp,None,None),None,None), None),None))'
    ot = translate_str_to_OT(in_str, 'moviedata')
    print(in_str)
    print(ot.print())
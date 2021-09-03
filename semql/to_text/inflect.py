
def plural_noun(noun):
    if noun == "":
        return ""

    if " of " in noun:
        sub_tokens = noun.split(" of ")
        head = plural_noun(sub_tokens[0])
        tail = " of ".join(sub_tokens[1:])
        return head + " of " + tail

    if noun.endswith('y'):
        if noun.endswith('day'):
            return noun + 's'
        return noun[:-1] + 'ies'
    elif noun == 'series':
        return 'series'
    elif noun.endswith("s"):
        return noun + 'es'
    elif noun.endswith("sh"):
        return noun + "es"
    elif noun == 'person':
        return 'people'
    else:
        return noun + 's'


def ordinal(num: str):
    assert num.isnumeric()

    last_digit = int(num[-1])
    second_to_last_digit = int(num[-2])

    if last_digit == 1:
        if second_to_last_digit == 1:
            return f"{num}th"
        else:
            return f"{num}st"
    elif last_digit == 2:
        if second_to_last_digit == 1:
            return f"{num}th"
        else:
            return f"{num}nd"
    elif last_digit == 3:
        if second_to_last_digit == 1:
            return f"{num}th"
        else:
            return f"{num}rd"
    else:
        return f"{num}th"


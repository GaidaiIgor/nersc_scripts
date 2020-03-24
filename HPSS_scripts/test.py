import sys

def flat_list(list_to_flat):
    if not isinstance(list_to_flat, list):
        yield list_to_flat
    else:
        for item in list_to_flat:
            yield from flat_list(item)


# A = ['str1', [[[['str2']]]], [['str3'], 'str4'], 'str5']
A = [1.0, 2, 'a', [4,], [[6,], [8,]], [[[8,],[9,]], [[12,],[10]]]]
print(list(flat_list(A)))
print(isinstance('asd', list))
from typing import List


def forever_iter(i_list: List) -> iter:
    while True:
        for i in i_list:
            yield i

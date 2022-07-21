from collections.abc import Iterable
import argparse

"""Script to remove duplicate lines from any txt file
to run python3 duplicate.py file.txt
"""


# function for converting nested lists to one dimensional list
def flatten(lis):
    for item in lis:
        if isinstance(item, Iterable) and not isinstance(item, str):
            for x in flatten(item):
                yield x
        else:
            yield item


def remove_duplicate(lst2):
    res = []
    for i in lst2:
        if i not in res:
            res.append(i)
    return res


def main(testfile):
    a_file = open(testfile, 'r', encoding='UTF8')

    lol = [(line.strip()).split() for line in a_file]  # converting txt file lines into list of lists

    a_file.close()
    var1 = list(flatten(lol))

    var2 = remove_duplicate(var1)
    final = open(testfile, "w")
    for out in var2:
        var3 = out + "\n"
        final.write(var3)


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()

    argparser.add_argument('testfile', type=str, nargs=1,
                           help='path to testfile to be processed.')
    args = argparser.parse_args()

    main(args.testfile[0])

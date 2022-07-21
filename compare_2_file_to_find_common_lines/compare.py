from collections.abc import Iterable

"""this script would print out all the common lines 
old file vs new file, script will print out all the common lines new file has with respect to old file
"""


# function for converting nested lists to one dimensional list
def flatten(lis):
    for item in lis:
        if isinstance(item, Iterable) and not isinstance(item, str):
            for x in flatten(item):
                yield x
        else:
            yield item


old = open("oldfile.txt", "r")

lol = [(line.strip()).split() for line in old]  # converting txt file lines into list of lists

old.close()

new = open("newfile.txt", "r")

lol1 = [(line.strip()).split() for line in new]  # converting txt file lines into list of lists

new.close()

new1 = list(flatten(lol1))
old1 = list(flatten(lol))

# with open("outputfile.txt", "w") as f:
for i in new1:
    if i not in old1:
        print(i)
        # f.write(i + "\n")

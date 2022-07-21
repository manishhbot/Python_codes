import socket
from collections import Iterable
import argparse

""" this script would convert hostnames to ip_address
    have hostnames/domains in a txt file(one hostname/domain in a line)
    to run - python3 hostname-to-ip.py domainfile.txt
"""


# function for converting nested lists to one dimensional list
def flatten(lis):
    for item in lis:
        if isinstance(item, Iterable) and not isinstance(item, str):
            for x in flatten(item):
                yield x
        else:
            yield item


def main(testfile):
    a_file = open(testfile, 'r')

    lol = [(line.strip()).split() for line in a_file]  # converting txt file lines into list of lists

    a_file.close()
    var1 = list(flatten(lol))

    with open(testfile, "w") as final:
        for hostname in var1:
            lot = []
            try:
                ip_addresses = [str(socket.gethostbyname(hostname))]
                lot.append(hostname)
                print(ip_addresses, hostname)
            except socket.gaierror:
                ip_addresses = False

            if ip_addresses:
                for out in lot:
                    var3 = out + "\n"
                    final.write(var3)
            else:
                pass


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()

    argparser.add_argument('testfile', type=str, nargs=1,
                           help='path to testfile to be processed.')
    args = argparser.parse_args()

    main(args.testfile[0])

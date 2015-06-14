#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function
import fileinput
from itertools import groupby, imap
import random

def mapper(line):
    if random.random() < possibility:
        yield line.strip()
        
def reducer(key, values):
    yield None
    
combiner = reducer

def do_mapper(files):
    for line in fileinput.input(files):
        for key, value in mapper(line):
            yield key, value

def do_combiner(mapper_out):
    mapper_out = sorted(mapper_out, key = lambda x: x[0])
    for key, grouped_keyvalues in groupby(mapper_out,
                                          key = lambda x: x[0]):
        values = (v for k, v in grouped_keyvalues)
        for key, value in combiner(key, values):
            yield key, value

def do_reducer(files):
    def line_to_keyvalue(line):
        key, value = line.strip().decode('utf8').split('\t', 1)
        return key, value

    keyvalues = imap(line_to_keyvalue, fileinput.input(files))
    for key, grouped_keyvalues in groupby(keyvalues,
                                          key=lambda x: x[0]):
        values = (v for k, v in grouped_keyvalues)
        for key, value in reducer(key, values):
            yield key, value
            


def argparser():
    import argparse
    parser = argparse.ArgumentParser(description='N-gram counter')
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        '-r', '--reducer', action='store_true', help='reducer mode')
    mode_group.add_argument(
        '-m', '--mapper', action='store_true', help='mapper mode')
    parser.add_argument('files', metavar='FILE', type=str, nargs='*',
                        help='input files')
    parser.add_argument('-p','--possibility',metavar='FILE', type=int,
                        help='sample ratio')
    return parser.parse_args()

if __name__ == '__main__':

    args = argparser()
    if args.mapper:
        mapper_out = do_mapper(args.files)
        # mapper_out = do_combiner(mapper_out)
        for key, value in mapper_out:
            print('{}\t{}'.format(key, value))
            
    elif args.reducer:
        reducer_out  = do_reducer(args.files)
        for key, value in reducer_out:
            print('{}\t{}'.format(key, value))

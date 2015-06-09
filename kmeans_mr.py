#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function
import fileinput
from itertools import groupby, imap
import numpy as np
import theano.tensor as T
from theano import function
import sys


c = T.dmatrix('c')
v = T.dvector('v')
min_euclidean_distances_index = function([c, v], T.sqrt(T.sqr(c - v).sum(axis=1)).argmin())


def str_to_id_vector(string):
    _id, vector = string.split('\t',1)
    return _id, np.fromiter((float(number) for number in vector.split()), dtype=np.float)



def read_centroids(centroids_file):
    centroids = []
    with open(centroids_file) as cf:
        for i, line in enumerate(cf):
            cluster_id, c = str_to_id_vector(line)
            assert int(cluster_id) == i
            centroids.append(c)
    # sorted_centroids = sorted(centroids, key = lambda x: x[0])
    return np.array(centroids)

# print(centroids)

def mapper(line):
    global centroids
    user_id, user_vector = str_to_id_vector(line)
    cluster = min_euclidean_distances_index(centroids, user_vector)
    yield cluster, line.strip()


def reducer(key, values):
    vectors = np.array([str_to_id_vector(value)[1] for value in values])
    new_centorid = vectors.sum(axis=0) / vectors.shape[0]
    yield key, ' '.join(str(v)for v in new_centorid)

# combiner = reducer


def do_mapper(files):
    for line in fileinput.input(files):
        for key, value in mapper(line):
            yield key, value


# def do_combiner(mapper_out):
#     mapper_out = sorted(mapper_out, key=lambda x: x[0])
#     for key, grouped_keyvalues in groupby(mapper_out,
#                                           key=lambda x: x[0]):
#         values = (v for k, v in grouped_keyvalues)
#         for key, value in combiner(key, values):
#             yield key, value


def do_reducer(files):
    def line_to_keyvalue(line):
        key, value = line.decode('utf8').split('\t', 1)
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
    parser.add_argument('--centroids-file', help='centroids vectors file')

    return parser.parse_args()

if __name__ == '__main__':

    args = argparser()
    if args.centroids_file:
        centroids = read_centroids(args.centroids_file)
        print(centroids, file=sys.stderr)
    if args.mapper:
        mapper_out = do_mapper(args.files)
        # mapper_out = do_combiner(mapper_out)
        for key, value in mapper_out:
            print('{}\t{}'.format(key, value))

    elif args.reducer:
        reducer_out = do_reducer(args.files)
        for key, value in reducer_out:
            print('{}\t{}'.format(key, value))

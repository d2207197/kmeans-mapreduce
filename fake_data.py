#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import numpy as np


import argparse

def argparser():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('n', type=int)
    parser.add_argument('m', type=int)
    return parser.parse_args()

        
    
if __name__ == '__main__':
    args = argparser()
    n, m = args.n, args.m
    for i, vector in enumerate(np.random.randn(n, m)):
        print('{}\t{}'.format(i,
                              ' '.join(str(v) for v in vector)))

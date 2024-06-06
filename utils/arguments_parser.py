# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Parser for the arguments passed to the benchmark driver"""
import argparse


def get_parser():
    parser = argparse.ArgumentParser(description='CLI for SparkOptimizer')
    parser.add_argument('--test', help='Run baseline 5 times then run optimized 5 times', action='store_true', default=False)
    parser.add_argument('--debug', help='Change the log level to debug', action='store_true', default=False)
    return parser

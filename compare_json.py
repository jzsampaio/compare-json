from sys import argv
import json
import os
import os.path
from numbers import Number
from glob import glob

from pandas import DataFrame, read_csv


def read_json(file_path):
    with open(file_path) as f:
        return json.load(f)


def _flatten(path, value):
    """
    Returns the flattened version of the given object, starting with the given
    path.
    For instance:
        _flatten((), {'foo': {'bar': 123}, 'bum': [1, 2, 3]}) =
        [
            (('foo', 'bar'), 123),
            (('bum', 0),       1),
            (('bum', 1),       2),
            (('bum', 2),       3),
        ]
    """
    if isinstance(value, dict):
        return sum([
            _flatten((*path, k), v)
            for k, v in value.items()
        ], [])

    if isinstance(value, list):
        return sum([
            _flatten((*path, i), v)
            for i, v in enumerate(value)
        ], [])

    return [(path, value)]


def flatten(value):
    return dict(_flatten((), value))


def split_set(a, b):
    common = a & b
    missing = a - b
    extra = b - a
    return (common, missing, extra)


def to_key(k):
    return '-'.join([str(x) for x in k])

def flatten_and_merge(source, target):
    flat_source = flatten(source)
    flat_target = flatten(target)

    common, missing, extra = split_set(flat_target.keys(), flat_source.keys())

    return [
        *[(to_key(k), True,  True,  flat_target[k], flat_source[k]) for k in common],
        *[(to_key(k), True,  False, flat_target[k], None) for k in missing],
        *[(to_key(k), False, True,  None,           flat_source[k]) for k in extra],
    ]


def diff(row):
    is_diff = not (row.in_target and row.in_source) or row.target_val != row.source_val
    abs_diff = ''
    rel_diff = ''

    if isinstance(row.target_val, Number) and isinstance(row.source_val, Number):
        abs_diff = abs(row.source_val - row.target_val)
        if row.target_val != 0:
            rel_diff = abs_diff / row.target_val

    return [is_diff, abs_diff, rel_diff]


def compare_json_files(target, source):
    df = DataFrame(
        flatten_and_merge(
            target=read_json(target),
            source=read_json(source),
        ),
        columns=[
            'field',
            'in_target',
            'in_source',
            'target_val',
            'source_val',
        ]
    )

    d = df.apply(diff, axis='columns', result_type='expand')
    df['is_diff'] = d[0]
    df['abs_diff'] = d[1]
    df['rel_diff'] = d[2]

    # We only care about the differences
    return df



if __name__ == "__main__":
    argv.pop(0)
    A = argv.pop(0)
    B = argv.pop(0)

    print('# Comparing valuations')
    report = compare_json_files(A, B).to_csv(index=False)
    print(report)


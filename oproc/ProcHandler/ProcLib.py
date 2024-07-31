"""
Library to store functions for processing data.
"""
import os
import re
from .. import ConfigHandler as ch
from .. import newprint
import pandas as pd
import numpy as np


print = newprint()


def get_all_suffix(var: str, din: dict) -> dict:
    tag_suffix = ch.getval("tag_suffix")
    if tag_suffix not in var:
        raise ValueError(f"tag suffix is not in var {var}")
    bval = var.replace(tag_suffix, '')
    i = 1
    dout = {}
    while True:
        test_str = bval + str(i)
        try:
            dout[test_str] = din[test_str]
        except KeyError:
            break
        i = i+1
    if not dout:
        raise ValueError(f"suffix value {var} not in data")
    else:
        return dout


def require_vars(var_list: list, kwargs: dict):
    if not isinstance(var_list, list):
        var_list = [var_list]
    for var in var_list:
        tests = list(kwargs.keys())
        tag_suffix = ch.getval("tag_suffix")
        if tag_suffix in var:
            to = []
            for test in tests:
                try:
                    to.append(test.replace(
                        re.search(r'(?=\d)\w+', test).group(), tag_suffix))
                except AttributeError:
                    to.append(test)
            tests = to
        present = False
        for k in tests:
            if k == var:
                present = True
        if present is False:
            raise ValueError(f'variable {var} not present in kwargs')
    print('Var check passed')
    return


def not_require_vars(var_list: list, kwargs: dict):
    try:
        require_vars(var_list, kwargs)
    except ValueError:
        print('Var check passed')
        return
    raise ValueError(f'Output variable already in struct')



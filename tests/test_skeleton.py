# -*- coding: utf-8 -*-

import pytest
from pyomop.skeleton import fib

__author__ = "Bell Eapen"
__copyright__ = "Bell Eapen"
__license__ = "none"


def test_fib():
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)

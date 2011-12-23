#!/usr/bin/env python
# Copyright (c) 2011 Instagram All rights reserved.
# This module is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.

import os

from setuptools import setup
setup(name='chashring',
      version = '0.1',
      author="Mike Krieger",
      author_email="mike@instagram.com",
      url="http://github.com/Instagram/",
      platforms=["Any"],
      py_modules = ['chashring'],
      license="BSD",
      keywords='hashing hash consistent',
      description="Python bindings for C hash-ring library")
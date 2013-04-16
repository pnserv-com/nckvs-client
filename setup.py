# -*- coding: utf-8 -*-

import os

from setuptools import setup, find_packages

version = '0.0.0'

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

requires = []

tests_require = requires + [
    'pytest',
    'mock'
]

setup(name='nckvs-client',
      version=version,
      description='A python client library for NCKVS',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
          "Programming Language :: Python",
          "Programming Language :: Python :: 3",
          "Topic :: Internet :: WWW/HTTP"
      ],
      author='Yoshihisa Tanaka',
      author_email='yoshihisa@iij.ad.jp',
      url='https://github.com/tin-com/nckvs-client',
      keywords='web',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=True,
      install_requires=requires,
      tests_require=tests_require)

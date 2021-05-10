#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
      long_description = f.read()

setup(name="pipelinewise-target-vertica",
      version="1.0.0",
      description="Singer.io target for loading data to Vertica - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="TransferWise",
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=["target_vertica"],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'vertica-python==1.0.1',
          'inflection==0.3.1',
          'joblib==1.0.0'
      ],
      extras_require={
          "test": [
              'nose==1.3.7',
              'mock==3.0.5',
              'pylint==2.4.4',
              'nose-cov==1.6'
            ]
      },
      entry_points="""
          [console_scripts]
          target-vertica=target_vertica:main
      """,
      packages=["target_vertica"],
      package_data = {},
      include_package_data=True,
)

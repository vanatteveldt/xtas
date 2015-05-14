#!/usr/bin/env python

from distutils.core import setup

setup(
    name="xtas",
    description="Distributed text analysis suite",
    author="Lars Buitinck",
    author_email="l.buitinck@esciencecenter.nl",
    packages=["xtas", "xtas.webserver"],
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Text Processing",
    ],
    install_requires=[
        "celery>=3.0.0",
        "flask>=0.10.1",
        "nltk",
        "elasticsearch",
    ],
    extras_requires={
        "clustering": "scikit-learn>=0.13",
        "fast": "librabbitmq",
        "lda": "gensim",
    },
)

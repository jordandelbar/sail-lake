"""Earthquake transformations: Spark DataFrame in, DataFrame out. No Dagster imports here.

Callers (Dagster assets, tests, notebooks) choose inputs and outputs; this package only says how
the data is derived. It targets the Spark API, which Sail and Spark both implement.
"""

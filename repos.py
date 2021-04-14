import csv
import os

from dagster import solid, pipeline

@solid
def hello_cereal(context):
    dataset_path = os.path.join(os.path.dirname(__file__), 'cereal.csv')
    with open(dataset_path, 'r') as f:
        cereals = [row for row in csv.DictReader(f)]
    context.log.info(f'Found {len(cereals)} cereals.')

@pipeline
def hello_cereal_pipeline():
    hello_cereal()
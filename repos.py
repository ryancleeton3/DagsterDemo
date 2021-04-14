import csv
import os

from dagster import solid, pipeline

@solid
def load_cereals(context):
    dataset_path = os.path.join(os.path.dirname(__file__), 'cereal.csv')
    with open(dataset_path, 'r') as f:
        cereals = [row for row in csv.DictReader(f)]
    context.log.info(f'Found {len(cereals)} cereals.')
    return cereals

@solid
def sort_by_calories(context, cereals):
    sorted_cereals = list(sorted(cereals, key=lambda cereal: cereal['calories']))
    context.log.info(f'Most Caloric Cereal: {sorted_cereals[-1]["name"]}')

@pipeline
def serial_pipeline():
    sort_by_calories(load_cereals())
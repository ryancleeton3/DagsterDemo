import csv
import os

from dagster import solid, pipeline

# for testing
from dagster import execute_pipeline

@solid(config_schema={'csv_name':str})
def load_cereals(context):
    dataset_path = os.path.join(os.path.dirname(__file__), context.solid_config['csv_name'])
    with open(dataset_path, 'r') as f:
        cereals = [row for row in csv.DictReader(f)]
    context.log.info(f'Found {len(cereals)} cereals.')
    return cereals

@solid
def sort_by_calories(context, cereals):
    most_caloric = list(sorted(cereals, key=lambda cereal: cereal['calories']))
    context.log.info(f'Most Caloric Cereal: {most_caloric[-1]["name"]}')
    return most_caloric

@solid
def sort_by_protein(context, cereals):
    sorted_cereals = list(sorted(cereals, key=lambda cereal: cereal['protein']))
    context.log.info(f'Most Protein-rich Cereal: {sorted_cereals[-1]["name"]}')
    return sorted_cereals

@solid
def display_results(context, most_calories, most_protein):
    context.log.info(f'Most Caloric Cereal: {most_calories}')
    context.log.info(f'Most Protein-rich Cereal: {most_protein}')

@solid
def clean_results(context, results):
    return results[-1]['name']

@pipeline
def complex_pipeline():
    cereals = load_cereals()
    most_caloric = sort_by_calories(cereals)
    most_protein_rich = sort_by_protein(cereals)
    display_results(clean_results(most_caloric), clean_results(most_protein_rich))

def test_complex_pipeline():
    run_config = {
        'solids': {
            'load_cereals': {
                'config': {
                    'csv_name': 'cereal.csv'
                }
            }
        }
    }
    res = execute_pipeline(complex_pipeline, run_config=run_config)
    assert res.success
    assert len(res.solid_result_list) == 6
    for solid_res in res.solid_result_list:
        assert solid_res.success
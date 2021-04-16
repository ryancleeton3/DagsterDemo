import csv
import os

from dagster import solid, pipeline, lambda_solid, DagsterType, Output, OutputDefinition, EventMetadataEntry, TypeCheck, repository

### for testing
from dagster import execute_pipeline, execute_solid, DagsterEventType, ExpectationResult, PipelineExecutionResult

def is_list_of_dicts(context, value):
    if not isinstance(value, list):
        return TypeCheck(
            success=False,
            description=f'LessSimpleDataFrame should be a list of dicts, got {type_}',
        )
    fields = [field for field in value[0].keys()]

    for i in range(len(value)):
        row = value[i]
        if not isinstance(row, dict):
            return TypeCheck(
                success=False,
                description=f'LessSimpleDataFrame should be a list of dicts, got {type(row)} for row {i+1}'
            )
        row_fields = [field for field in row.keys()]
        if fields != row_fields:
            return TypeCheck(
                success=False,
                description=f'Rows in LessSimpleDataFrame should have the same fields, got {row_fields}, for row {i+1}, expected {fields}',
            )

    return TypeCheck(
        success=True,
        description="LessSimpleDataFrame summary statistics",
        metadata_entries=[
            EventMetadataEntry.text(
                str(len(value)),
                'n_rows',
                'Number of rows seen in the data frame'
            ),
            EventMetadataEntry.text(
                str(len(value[0].keys()) if len(value) > 0 else 0),
                'n_cols',
                'Number of columns seen in the data frame'
            ),
            EventMetadataEntry.text(
                str(list(value[0].keys()) if len(value) > 0 else []),
                'column_names',
                'Keys of columns seen in the data frame'
            )
        ]
    )

SimpleDataFrame = DagsterType(
    name='SimpleDataFrame',
    type_check_fn=is_list_of_dicts,
    description='A naive representation of a data frame, e.g., as returned by csv.'
)

### Solids
@solid(
    output_defs=[OutputDefinition(SimpleDataFrame)]
)
def load_cereals(context, csv_path: str):
    dataset_path = os.path.join(os.path.dirname(__file__), csv_path)
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
def display_results(context, hot_most_calories, cold_most_calories, most_protein):
    context.log.info(f'Most Caloric Hot Cereal: {hot_most_calories}')
    context.log.info(f'Most Caloric cold Cereal: {cold_most_calories}')
    context.log.info(f'Most Protein-rich Cereal: {most_protein}')

@lambda_solid
def clean_results(results) -> str:
    return results[-1]['name']

@solid(
    output_defs=[
        OutputDefinition(name='cold_cereal'),
        OutputDefinition(name='hot_cereal')
    ]
)
def split_cereals(context, cereals):
    cold_cereal = [cereal for cereal in cereals if cereal['type']=='C']
    hot_cereal = [cereal for cereal in cereals if cereal['type']=='H']
    yield Output(cold_cereal, output_name='cold_cereal')
    yield Output(hot_cereal, output_name='hot_cereal')

### Pipeline
@pipeline
def complex_pipeline():
    cereals = load_cereals()
    cold_cereal, hot_cereal = split_cereals(cereals)
    sort_hot_most_caloric = sort_by_calories.alias('sort_hot_most_caloric')
    sort_cold_most_caloric = sort_by_calories.alias('sort_cold_most_caloric')
    clean_hot_most_caloric = clean_results.alias('clean_hot_most_caloric')
    clean_cold_most_caloric = clean_results.alias('clean_cold_most_caloric')



    most_protein_rich = sort_by_protein(cereals)
    clean_most_protein = clean_results.alias('clean_most_protein')
    display_results(clean_hot_most_caloric(sort_hot_most_caloric(hot_cereal)), clean_cold_most_caloric(sort_cold_most_caloric(cold_cereal)), clean_most_protein(most_protein_rich))

### Repositories
@repository
def hello_cereal_repository():
    return {
        'pipelines': {
            'complex_pipeline': lambda: complex_pipeline,
        }
    }

### TESTS

def test_complex_pipeline():
    run_config = {
        'solids': {
            'load_cereals': {
                'inputs': {
                    'csv_path': {
                        'value': 'cereal.csv'
                    }
                }
            }
        }
    }
    res = execute_pipeline(complex_pipeline, run_config=run_config)
    assert isinstance(res, PipelineExecutionResult)
    assert res.success
    assert len(res.solid_result_list) == 9
    for solid_res in res.solid_result_list:
        assert solid_res.success

    load_cereal_solid_result = res.result_for_solid('load_cereals')

    assert [se.event_type for se in load_cereal_solid_result.step_events] == [
        DagsterEventType.STEP_START,
        DagsterEventType.STEP_INPUT,
        DagsterEventType.STEP_OUTPUT,
        DagsterEventType.HANDLED_OUTPUT,
        DagsterEventType.STEP_SUCCESS,
    ]

    sort_by_calories_solid_result = res.result_for_solid('sort_cold_most_caloric')

    assert [se.event_type for se in sort_by_calories_solid_result.step_events] == [
        DagsterEventType.STEP_START,
        DagsterEventType.LOADED_INPUT,
        DagsterEventType.STEP_INPUT,
        DagsterEventType.STEP_OUTPUT,
        DagsterEventType.HANDLED_OUTPUT,
        DagsterEventType.STEP_SUCCESS,
    ]

    assert type(res.output_for_solid('sort_cold_most_caloric')) == list
    assert type(res.output_for_solid('clean_cold_most_caloric')) == str 
    assert res.output_for_solid('clean_cold_most_caloric') == 'Strawberry Fruit Wheats'


def test_load_cereal():
    run_config = {
        'solids': {
            'load_cereals': {
                'inputs': {
                    'csv_path': {
                        'value': 'cereal.csv'
                    }
                }
            }
        }
    }
    res = execute_solid(load_cereals, run_config=run_config)
    assert res.success
    assert len(res.output_value()) == 77

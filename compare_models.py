import os
import sys
import subprocess
import argparse
from pathlib import Path
import datetime
import json
import csv
import tempfile
import shutil

def find_model_path(model_name):
    """Find the full path to a model."""
    try:
        # Find project root
        current = Path.cwd()
        while current != current.parent:
            if (current / 'dbt_project.yml').exists():
                project_root = current
                break
            current = current.parent
        else:
            print("Could not find dbt_project.yml")
            return None

        # If full path is provided
        if model_name.endswith('.sql'):
            path = Path(model_name)
            if path.exists():
                return path
            model_name = path.stem

        # Search for the model file in models directory
        models_dir = project_root / 'models'
        matches = list(models_dir.rglob(f"*{model_name}.sql"))
        
        if not matches:
            print(f"Could not find model {model_name}")
            return None
            
        if len(matches) > 1:
            print(f"Found multiple matches for {model_name}:")
            for match in matches:
                print(f"  {match}")
            print("Please specify the model more precisely")
            return None
            
        return matches[0]

    except Exception as e:
        print(f"Error in find_model_path: {str(e)}")
        return None

def get_main_branch_content(model_path):
    """Get content of the file from main branch."""
    try:
        # Get the git root directory
        git_root = subprocess.run(
            ['git', 'rev-parse', '--show-toplevel'],
            capture_output=True,
            text=True,
            check=True
        ).stdout.strip()
        
        # Convert model_path to be relative to git root
        git_root_path = Path(git_root)
        try:
            relative_path = model_path.relative_to(git_root_path)
        except ValueError:
            relative_path = model_path
        
        print(f"Looking for file in main branch at: {relative_path}")
        
        result = subprocess.run(
            ['git', 'show', f'main:{relative_path}'], 
            capture_output=True, 
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Warning: Could not find {relative_path} in main branch")
        print(f"Git error: {e.stderr.decode()}")
        return None
    except Exception as e:
        print(f"Error accessing main branch content: {str(e)}")
        return None

def create_temp_model(content, suffix, original_name, model_dir):
    """Create a temporary copy of the model."""
    try:
        temp_name = f"temp_{original_name}_{suffix}"
        analysis_dir = model_dir / 'analysis'
        analysis_dir.mkdir(exist_ok=True)
        temp_path = analysis_dir / f"{temp_name}.sql"

        # Add config block to ensure proper materialization
        config_block = '''{{
    config(
        materialized='table',
        schema='dev'
    )
}}

'''
        # Create the modified content
        modified_content = config_block + content.replace(f"ref('{original_name}')", f"ref('{temp_name}')")
        
        with open(temp_path, 'w') as f:
            f.write(modified_content)
        
        return temp_path, temp_name
        
    except Exception as e:
        print(f"Error creating temporary model: {e}")
        return None, None

def create_comparison_macro(model1_name: str, model2_name: str) -> Path:
    """Create a macro file for model comparison."""
    macro_content = '''
{% macro compare_versions() %}
    {% set relation1 = ref(\'''' + model1_name + '''\') %}
    {% set relation2 = ref(\'''' + model2_name + '''\') %}

    {% set cols1 = adapter.get_columns_in_relation(relation1) %}
    {% set cols2 = adapter.get_columns_in_relation(relation2) %}

    {% set common_cols = [] %}
    {% set version1_only_cols = [] %}
    {% set version2_only_cols = [] %}
    {% set type_changes = [] %}

    {# Find common and unique columns #}
    {% for col1 in cols1 %}
        {% set col_in_version2 = false %}
        {% for col2 in cols2 %}
            {% if col1.name|lower == col2.name|lower %}
                {% do common_cols.append(col1.name) %}
                {% set col_in_version2 = true %}
                {% if col1.dtype != col2.dtype %}
                    {% do type_changes.append({
                        'column': col1.name,
                        'main_type': col1.dtype,
                        'current_type': col2.dtype
                    }) %}
                {% endif %}
            {% endif %}
        {% endfor %}
        {% if not col_in_version2 %}
            {% do version1_only_cols.append(col1.name) %}
        {% endif %}
    {% endfor %}

    {% for col2 in cols2 %}
        {% set col_in_version1 = false %}
        {% for col1 in cols1 %}
            {% if col2.name|lower == col1.name|lower %}
                {% set col_in_version1 = true %}
            {% endif %}
        {% endfor %}
        {% if not col_in_version1 %}
            {% do version2_only_cols.append(col2.name) %}
        {% endif %}
    {% endfor %}

    {% set query %}
        with row_counts as (
            select
                count(*) as main_rows,
                '{{ version1_only_cols|join(", ") }}' as columns_removed,
                '{{ version2_only_cols|join(", ") }}' as columns_added
                {% for col in common_cols %}
                , count("{{ col }}") as main_{{ col }}_non_null
                , count(distinct "{{ col }}") as main_{{ col }}_distinct
                {% endfor %}
            from {{ relation1 }}
        ),
        current_counts as (
            select
                count(*) as current_rows
                {% for col in common_cols %}
                , count("{{ col }}") as current_{{ col }}_non_null
                , count(distinct "{{ col }}") as current_{{ col }}_distinct
                {% endfor %}
            from {{ relation2 }}
        )
        select
            r.main_rows,
            c.current_rows,
            c.current_rows - r.main_rows as row_difference,
            r.columns_removed,
            r.columns_added
            {% for col in common_cols %}
            , r.main_{{ col }}_non_null
            , c.current_{{ col }}_non_null
            , r.main_{{ col }}_distinct
            , c.current_{{ col }}_distinct
            {% endfor %}
        from row_counts r
        cross join current_counts c
    {% endset %}

    {% do log('MODEL COMPARISON RESULTS START', info=True) %}
    {% do log('Schema Changes:', info=True) %}
    {% do log('Common columns: ' ~ common_cols|join(', '), info=True) %}
    {% do log('Main branch only columns: ' ~ version1_only_cols|join(', '), info=True) %}
    {% do log('Current branch only columns: ' ~ version2_only_cols|join(', '), info=True) %}
    {% do log('Column type changes: ' ~ type_changes|tojson, info=True) %}
    {% do log('DATA COMPARISON:', info=True) %}
    {% set results = run_query(query) %}
    {% do results.print_table() %}
    {% do log('MODEL COMPARISON RESULTS END', info=True) %}

{% endmacro %}
'''
    
    macros_dir = Path('macros')
    macros_dir.mkdir(exist_ok=True)
    macro_path = macros_dir / 'compare_versions.sql'
    with open(macro_path, 'w') as f:
        f.write(macro_content)
    return macro_path

def get_immediate_downstream_models(model_name):
    """Get only the immediate downstream dependencies of the given model."""
    try:
        # Run dbt deps to generate manifest
        result = subprocess.run(
            ['dbt', 'deps'],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Use dbt list with '1' to get only immediate children
        result = subprocess.run(
            ['dbt', 'list', '--select', f'{model_name}+1'],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse output to get model names
        models = [line.strip() for line in result.stdout.split('\n') if line.strip()]
        # Remove the original model from the list
        models = [m for m in models if m != model_name]
        return models
        
    except subprocess.CalledProcessError as e:
        print(f"Error getting downstream models: {e}")
        return []

def save_results(results_json: str, output_dir: Path, model_name: str) -> Path:
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    result_dir = output_dir / f'{model_name}_comparison_{timestamp}'
    result_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Parse output sections
        schema_changes = []
        data_comparison = []
        in_results = False
        in_data = False
        
        for line in results_json.splitlines():
            if 'MODEL COMPARISON RESULTS START' in line:
                in_results = True
                continue
            elif 'DATA COMPARISON:' in line:
                in_data = True
                continue
            elif 'MODEL COMPARISON RESULTS END' in line:
                break
                
            if in_results:
                if in_data and '|' in line:
                    data_comparison.append(line)
                elif not in_data and ':' in line:
                    schema_changes.append(line)
        
        # Save text format
        with open(result_dir / 'model_comparison.txt', 'w') as f:
            f.write("SCHEMA CHANGES\n")
            f.write("==============\n")
            f.write("\n".join(schema_changes))
            f.write("\n\nDATA COMPARISON\n")
            f.write("===============\n")
            f.write("\n".join(data_comparison))
        
        # Save CSV format
        if data_comparison:
            # Process the table data
            table_data = []
            for line in data_comparison:
                if '|' in line:  # Only process lines that contain actual data
                    # Split by |, strip whitespace, and filter out empty strings
                    row = [col.strip() for col in line.split('|') if col.strip()]
                    if row:  # Only add non-empty rows
                        table_data.append(row)
            
            if len(table_data) >= 2:  # Need at least headers and one row
                csv_path = result_dir / 'model_comparison.csv'
                with open(csv_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(table_data[0])  # Headers
                    writer.writerows(table_data[1:])  # Data rows
                print(f"\nCSV results saved to: {csv_path}")
        
        print(f"\nResults saved to: {result_dir}")
        
        # Save raw output for debugging
        with open(result_dir / 'raw_output.txt', 'w') as f:
            f.write(results_json)
            
        return result_dir
        
    except Exception as e:
        print(f"Error saving results: {e}")
        print("\nRaw output:")
        print(results_json)
        return None



def main():
    parser = argparse.ArgumentParser(description='Compare dbt model versions')
    parser.add_argument('model_name', help='Name of the model to compare')
    parser.add_argument('--output-dir', type=Path, default=Path('model_comparisons'),
                       help='Directory to save comparison results')
    parser.add_argument('--include-downstream', action='store_true',
                       help='Also compare immediate downstream models')
    
    args = parser.parse_args()
    
    # Initialize paths as None
    main_path = None
    current_path = None
    macro_path = None
    
    try:
        # Find project root
        current = Path.cwd()
        while current != current.parent:
            if (current / 'dbt_project.yml').exists():
                project_root = current
                break
            current = current.parent
        
        model_path = find_model_path(args.model_name)
        if not model_path:
            sys.exit(1)
        
        print(f"Found model at: {model_path}")
        
        with open(model_path, 'r') as f:
            current_content = f.read()
        
        main_content = get_main_branch_content(model_path)
        if not main_content:
            sys.exit(1)
        
        model_dir = model_path.parent
        original_name = model_path.stem
        
        # Create temp models
        main_path, main_name = create_temp_model(
            main_content, 'main', original_name, model_dir)
        current_path, current_name = create_temp_model(
            current_content, 'current', original_name, model_dir)
        
        if not main_path or not current_path:
            print("Failed to create temporary models")
            sys.exit(1)
        
        print(f"Created temporary models: {main_name} and {current_name}")
        
        macro_path = create_comparison_macro(main_name, current_name)
        if not macro_path:
            print("Failed to create comparison macro")
            sys.exit(1)
        print("Created comparison macro")

        # Create temp dir for main branch files
        with tempfile.TemporaryDirectory() as main_dir:
            # Copy project structure to temp dir
            shutil.copytree(project_root, main_dir, dirs_exist_ok=True)
            
            print("\nGetting main branch version of models directory...")
            models_dir = Path(main_dir) / 'models'
            if models_dir.exists():
                shutil.rmtree(models_dir)
            
            try:
                # Get models directory from main branch
                subprocess.run(
                    ['git', 'checkout', 'main', '--', 'models'],
                    cwd=main_dir,
                    check=True
                )
                print("Successfully copied models from main branch")
            except subprocess.CalledProcessError as e:
                print(f"Error getting models from main branch: {e}")
                sys.exit(1)

            # Run main branch model from temp dir
            print("\nRunning main branch model...")
            main_result = subprocess.run(
                ['dbt', 'run', '--models', f"+{main_name}", 
                 '--target', 'dev',
                 '--full-refresh',
                 '--debug'],
                cwd=main_dir,
                capture_output=True,
                text=True,
                check=False
            )
            
            print("\nComplete command output:")
            print("=" * 50)
            print("STDOUT:")
            print(main_result.stdout or "No stdout")
            print("\nSTDERR:")
            print(main_result.stderr or "No stderr")
            print("=" * 50)
            
            if main_result.returncode != 0:
                print(f"\nFull error details:")
                print(f"Command: dbt run --models +{main_name} --target dev --full-refresh --debug")
                print(f"Return code: {main_result.returncode}")
                sys.exit(1)

            # Run current branch version
            print("\nRunning current branch model...")
            try:
                current_result = subprocess.run(
                    ['dbt', 'run', '--models', f"+{current_name}",
                     '--target', 'dev',
                     '--full-refresh'],
                    capture_output=True,
                    text=True,
                    check=True
                )
                print(current_result.stdout)
                
            except subprocess.CalledProcessError as e:
                print(f"Error running current branch model: {e}")
                print(e.stdout)
                print(e.stderr)
                sys.exit(1)
            
            # Run comparison
            print("\nComparing versions...")
            try:
                compare_result = subprocess.run(
                    ['dbt', 'run-operation', 'compare_versions', '--target', 'dev'],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                save_results(compare_result.stdout, args.output_dir, original_name)
                
            except subprocess.CalledProcessError as e:
                print(f"Error executing comparison: {e}")
                print(e.stdout)
                print(e.stderr)
                sys.exit(1)
            
            # Compare downstream models if requested
            if args.include_downstream:
                downstream_models = get_immediate_downstream_models(original_name)
                if downstream_models:
                    print(f"\nFound {len(downstream_models)} immediate downstream models:")
                    for model in downstream_models:
                        print(f"  - {model}")
                    
                    # Compare each downstream model
                    for model in downstream_models:
                        print(f"\nComparing downstream model: {model}")
                        downstream_path = find_model_path(model)
                        if not downstream_path:
                            print(f"Could not find downstream model: {model}")
                            continue
                            
                        # Create temporary models for downstream comparison
                        main_downstream_content = get_main_branch_content(downstream_path)
                        if not main_downstream_content:
                            print(f"Could not get main branch content for: {model}")
                            continue
                            
                        with open(downstream_path, 'r') as f:
                            current_downstream_content = f.read()
                            
                        # Create and run temporary downstream models
                        main_down_path, main_down_name = create_temp_model(
                            main_downstream_content, f'main_down_{model}', model, model_dir)
                        current_down_path, current_down_name = create_temp_model(
                            current_downstream_content, f'current_down_{model}', model, model_dir)
                            
                        if not main_down_path or not current_down_path:
                            print(f"Could not create temporary models for: {model}")
                            continue
                            
                        # Create comparison macro for downstream model
                        down_macro_path = create_comparison_macro(main_down_name, current_down_name)
                        if not down_macro_path:
                            print(f"Could not create comparison macro for: {model}")
                            continue
                        
                        try:
                            # Run downstream models
                            print(f"Running main branch version of: {model}")
                            subprocess.run(
                                ['dbt', 'run', '--models', f"{main_down_name}",
                                 '--target', 'dev',
                                 '--full-refresh'],
                                capture_output=True,
                                text=True,
                                check=True
                            )
                            
                            print(f"Running current branch version of: {model}")
                            subprocess.run(
                                ['dbt', 'run', '--models', f"{current_down_name}",
                                 '--target', 'dev',
                                 '--full-refresh'],
                                capture_output=True,
                                text=True,
                                check=True
                            )
                            
                            # Compare downstream results
                            print(f"Comparing versions of: {model}")
                            compare_result = subprocess.run(
                                ['dbt', 'run-operation', 'compare_versions',
                                 '--target', 'dev'],
                                capture_output=True,
                                text=True,
                                check=True
                            )
                            
                            # Save downstream comparison results
                            save_results(compare_result.stdout, 
                                       args.output_dir, 
                                       f"{model}_downstream")
                                       
                        finally:
                            # Cleanup downstream temporary files
                            for path in [main_down_path, current_down_path, down_macro_path]:
                                if path and path.exists():
                                    try:
                                        os.remove(path)
                                        print(f"Cleaned up temporary file: {path}")
                                    except Exception as e:
                                        print(f"Warning: Could not remove temporary file {path}: {e}")
                else:
                    print("\nNo immediate downstream models found.")
       
    finally:
        # Cleanup main temporary files
        for path in [main_path, current_path, macro_path]:
            if path and path.exists():
                try:
                    os.remove(path)
                    print(f"Cleaned up temporary file: {path}")
                except Exception as e:
                    print(f"Warning: Could not remove temporary file {path}: {e}")

if __name__ == "__main__":
    main()

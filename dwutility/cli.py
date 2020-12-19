import sys
import csv
import configparser
from datetime import datetime, timedelta
import time
import json
from pathlib import Path
import click
import snowflake.connector
from snowflake.connector import DictCursor

root_path = Path(__file__).parent.parent.resolve()
default_config_path = root_path / 'config.ini'

# utility functions

# read table meta data
def read_source_table_list(root_path):
    with open(root_path / 'metadata/mapping/source_tables.csv', encoding='utf-8-sig') as tables_file:
        tables = list(csv.DictReader(tables_file, delimiter=','))
    return tables

# read schema meta data
def read_source_schema_list(root_path):
    tables = read_source_table_list(root_path)
    schemas = set(table['schema'] for table in tables)
    return schemas

# Run statement
def execute_ddl(ctx, ddl):
    with ctx.cursor() as cs:
        cs.execute(ddl)
        one_row = cs.fetchone()
    return one_row[0]

# Execute all DDLs in a directory
def execute_all_ddls(ctx, dir_path, glob='*.sql'):
    """Deploy dv"""
    paths = list(dir_path.glob(glob))

    for path in paths:
        with open(path, encoding='utf-8') as ddl_file:
            ddl = ddl_file.read()
            print(ddl)
            result = execute_ddl(ctx, ddl)
            print(result)

# Run an SQL query.
# Only suitable for small query results, as the result is stored in memory
def execute_query(ctx, sql):
    with ctx.cursor(DictCursor) as cur:
        results = cur.execute(sql).fetchall()
    return results

# replaced reserved keywords from sql in table names
def replace_reserved_sql(name):
    if name == 'group':
        return 'groups'
    else:
        return name

# CLI commands

@click.group()
@click.option('--config', help='The config file', type=click.Path(exists=True), default=default_config_path, show_default=True)
@click.option('-v', '--verbose', help='Print extra information', count=True)
@click.pass_context
def cli(ctx, config, verbose):
    """ DW Utility"""
    config_ = configparser.ConfigParser()
    config_.read(config)
    ctx.ensure_object(dict)
    ctx.obj['SNOWFLAKE_CONFIG'] = dict(config_['snowflake'])
    ctx.obj['S3_CONFIG'] = dict(config_['s3'])
    ctx.obj['VERBOSE'] = verbose

@cli.group()
def integration():
    """New source integration"""
    pass

@integration.command('schema')
@click.pass_context
def integration_schema(ctx):
    """Integration schema"""
    snowflake_config = ctx.obj['SNOWFLAKE_CONFIG']
    # read meta data
    schemas = read_source_schema_list(root_path)

    # Snowflake connection
    with snowflake.connector.connect(**snowflake_config) as sqlSnowflake:
        for schema in schemas:
            ddl = '''
create schema if not exists {schema};
            '''.format(schema=schema)
            print(ddl) 
            result = execute_ddl(ctx=sqlSnowflake, ddl=ddl)
            print(result)

@integration.command('file_format')
@click.pass_context
def integration_file_format(ctx):
    """Integration file format"""
    snowflake_config = ctx.obj['SNOWFLAKE_CONFIG']
    # read meta data
    schemas = read_source_schema_list(root_path)

    # Snowflake connection
    with snowflake.connector.connect(**snowflake_config) as sqlSnowflake:
        for schema in schemas:
            ddl = '''
create file format if not exists {schema}.jsonl
    type = 'json' compression = 'auto' enable_octal = false allow_duplicate = false 
    strip_outer_array = false strip_null_values = false ignore_utf8_errors = true;
            '''.format(schema=schema)
            print(ddl) 
            result = execute_ddl(ctx=sqlSnowflake, ddl=ddl)
            print(result)

@integration.command('table')
@click.pass_context
def integration_table(ctx):
    """Integration create table"""
    snowflake_config = ctx.obj['SNOWFLAKE_CONFIG']

    # read meta data
    tables = read_source_table_list(root_path)

    # Snowflake connection
    with snowflake.connector.connect(**snowflake_config) as sqlSnowflake:
        for table in tables:
            ddl = '''
create table if not exists {schema}.{table}
(
src variant
);  
            '''.format(schema=table['schema'], table=replace_reserved_sql(table['table']))
            print(ddl) 
            result = execute_ddl(ctx=sqlSnowflake, ddl=ddl)
            print(result)

@integration.command('copy')
@click.option('--schema', help='Schema to get table defs from')
@click.option('--table', help='Table to get info from')
@click.pass_context
def integration_copy(ctx, schema, table):
    """Integration copy into - initial load from internal stage - will create copies if run many times"""
    snowflake_config = ctx.obj['SNOWFLAKE_CONFIG']

    # read meta data
    tables = read_source_table_list(root_path)
    if schema:
        tables = [t for t in tables if t['schema'].lower() == schema.lower()]
    if table:
        tables = [t for t in tables if t['table'].lower() == table.lower()]
    
    # Snowflake connection
    with snowflake.connector.connect(**snowflake_config) as sqlSnowflake:
        for table in tables:
            ddl = '''
copy into {schema}.{target_table}
from @stage_adventureworks2019/{src_table}.jsonl.gz;  
            '''.format(schema=table['schema'], src_table=table['table'], target_table=replace_reserved_sql(table['table']))
            print(ddl) 
            result = execute_ddl(ctx=sqlSnowflake, ddl=ddl)
            print(result)

@integration.command('all')
@click.pass_context
def integration_all(ctx):
    """Integration all"""
    ctx.forward(integration_schema)
    ctx.forward(integration_file_format)
    ctx.forward(integration_table)
    ctx.forward(integration_copy) # dangerous

@cli.group()
def drop():
    """Drop objects on Snowflake"""
    pass

@drop.command('snowpipe')
@click.pass_context
def drop_snowpipe(ctx):
    """Drop snowpipe reading data from metadata/source_tables.csv"""
    snowflake_config = ctx.obj['SNOWFLAKE_CONFIG']

    # do stuff
    tables = read_source_table_list(root_path)

    # Snowflake connection
    with snowflake.connector.connect(**snowflake_config) as sqlSnowflake:
        for table in tables:
            # generate dropping snowpipes in Snowflake
            ddl = '''
drop pipe {schema}.{target_table};
            '''.format(schema=table['schema'], src_table=table['table'], target_table=replace_reserved_sql(table['table']))
            print(ddl) 
            result = execute_ddl(ctx=sqlSnowflake, ddl=ddl)
            print(result)

@cli.group()
def deploy():
    """Deploy DDLs on Snowflake"""
    pass

@deploy.command('stage')
@click.pass_context
def deploy_stage(ctx):
    """Deploy stage"""
    snowflake_config = ctx.obj['SNOWFLAKE_CONFIG']
    dir_path = root_path / 'sql/dw/stage/'
    with snowflake.connector.connect(**snowflake_config) as ctx:
        execute_all_ddls(ctx, dir_path)

@deploy.command('dv')
@click.pass_context
def deploy_dv(ctx):
    """Deploy dv"""
    snowflake_config = ctx.obj['SNOWFLAKE_CONFIG']
    dir_path = root_path / 'sql/dw/dv/'
    with snowflake.connector.connect(**snowflake_config) as ctx:
        # deploy ref tables
        execute_all_ddls(ctx, dir_path, 'ref_*_t.sql')    
        # deploy non buisines views
        execute_all_ddls(ctx, dir_path, '*_*[!b]_v.sql')
        # deploy buisines views
        execute_all_ddls(ctx, dir_path, '*_*b_v.sql')

@deploy.command('analytics')
@click.pass_context
def deploy_analytics(ctx):
    """Deploy analytics"""
    snowflake_config = ctx.obj['SNOWFLAKE_CONFIG']
    dir_path = root_path / 'sql/dw/analytics/'
    with snowflake.connector.connect(**snowflake_config) as ctx:
        execute_all_ddls(ctx, dir_path, '*_[vt].sql')


@deploy.command('all')
@click.pass_context
def deploy_all(ctx):
    """Deploy all"""
    ctx.forward(deploy_stage)
    ctx.forward(deploy_dv)
    ctx.forward(deploy_analytics)
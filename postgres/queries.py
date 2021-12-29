from textwrap import indent, dedent


def create_temp_table_query(schema, staging_table_name):
    schema_str = indent(schema.sql_string, '    ')
    return '\n'.join([
        dedent("""
        DROP TABLE IF EXISTS {staging_table_name};
        CREATE TEMP TABLE {staging_table_name} (
        """).strip().format(staging_table_name=staging_table_name),
        schema_str,
        ")"])


def create_table_if_not_exists_query(table_schema, table_name):
    schema_str = indent(table_schema.sql_string, '    ')
    return '\n'.join([
        dedent("""
            CREATE TABLE IF NOT EXISTS {table_name} (
            """).strip().format(table_name=table_name),
        schema_str,
        ")"])


def truncate_query(schema, table_name):
    return "TRUNCATE {}.{}".format(schema, table_name)


def upsert_query(source_table_name, target_table_name, selector_fields, setter_fields):
    sql_template = dedent("""
            WITH updates AS (
                UPDATE %(target)s t
                    SET \n%(set)s        
                FROM %(source)s s
                WHERE %(where_t_pk_eq_s_pk)s 
                RETURNING %(s_pk)s
            )
            INSERT INTO %(target)s (%(columns)s)
                SELECT %(source_columns)s 
                FROM %(source)s s LEFT JOIN updates t USING(%(pk)s)
                WHERE %(where_t_pk_is_null)s""")
    statement = sql_template % dict(
        target=target_table_name,
        set=',\n'.join([indent("%s = s.%s", '                    ') % (x, x) for x in setter_fields]),
        source=source_table_name,
        where_t_pk_eq_s_pk=' AND '.join(["t.%s = s.%s" % (x, x) for x in selector_fields]),
        s_pk=','.join(["s.%s" % x for x in selector_fields]),
        columns=','.join([x for x in selector_fields + setter_fields]),
        source_columns=','.join(['s.%s' % x for x in selector_fields + setter_fields]),
        pk=','.join(selector_fields),
        where_t_pk_is_null=' AND '.join(["t.%s IS NULL" % x for x in selector_fields]),
        t_pk=','.join(["t.%s" % x for x in selector_fields]))
    return dedent(statement).strip()
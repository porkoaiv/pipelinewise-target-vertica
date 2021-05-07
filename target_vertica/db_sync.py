import json
import sys
import uuid
import time
from singer import get_logger
import vertica_python as vertica

from target_vertica.utils import (
    add_columns,
    column_clause,
    column_type,
    flatten_schema,
    flatten_record,
    format_json,
    primary_column_names,
    safe_column_name,
    stream_name_to_dict,
    validate_config, 
)


# pylint: disable=too-many-public-methods,too-many-instance-attributes
class DbSync:
    def __init__(self, connection_config, stream_schema_message=None):
        # [CHECK] - OK SO FAR
        """
            connection_config:      Vertica connection details

            stream_schema_message:  An instance of the DbSync class is typically used to load
                                    data only from a certain singer tap stream.

                                    The stream_schema_message holds the destination schema
                                    name and the JSON schema that will be used to
                                    validate every RECORDS messages that comes from the stream.
                                    Schema validation happening before creating CSV and before
                                    uploading data into Vertica.

                                    If stream_schema_message is not defined then we can use
                                    the DbSync instance as a generic purpose connection to
                                    Vertica and can run individual queries. For example
                                    collecting catalog information from Vertica for caching
                                    purposes.
        """
        self.connection_config = connection_config
        self.stream_schema_message = stream_schema_message

        # logger to be used across the class's methods
        self.logger = get_logger('target_vertica')

        # Validate connection configuration
        config_errors = validate_config(connection_config)

        # Exit if config has errors
        if len(config_errors) > 0:
            self.logger.error("Invalid configuration:\n   * %s", '\n   * '.join(config_errors))
            sys.exit(1)

        self.schema_name = None
        self.grantees = None

        # Init stream schema
        if stream_schema_message is not None:
            # Define initial list of indices to created
            self.hard_delete = self.connection_config.get('hard_delete')
            if self.hard_delete:
                self.indices = ['_sdc_deleted_at']
            else:
                self.indices = []

            #  Define target schema name.
            #  --------------------------
            #  Target schema name can be defined in multiple ways:
            #
            #   1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
            #       not specified explicitly for a given stream in the `schema_mapping` object
            #   2: 'schema_mapping' key : Target schema defined explicitly for a given stream.
            #       Example config.json:
            #           "schema_mapping": {
            #               "my_tap_stream_id": {
            #                   "target_schema": "my_vertica_schema",
            #                   "target_schema_select_permissions": [ "role_with_select_privs" ],
            #                   "indices": ["column_1", "column_2s"]
            #               }
            #           }

            config_default_target_schema = self.connection_config.get('default_target_schema', '').strip()
            config_schema_mapping = self.connection_config.get('schema_mapping', {})

            stream_name = stream_schema_message['stream']
            stream_schema_name = stream_name_to_dict(stream_name)['schema_name']
            stream_table_name = stream_name_to_dict(stream_name)['table_name']
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.schema_name = config_schema_mapping[stream_schema_name].get('target_schema')

                # Get indices to create for the target table
                indices = config_schema_mapping[stream_schema_name].get('indices', {})
                if stream_table_name in indices:
                    self.indices.extend(indices.get(stream_table_name, []))

            elif config_default_target_schema:
                self.schema_name = config_default_target_schema

            if not self.schema_name:
                raise Exception(
                    "Target schema name not defined in config. "
                    "Neither 'default_target_schema' (string) nor 'schema_mapping' (object) defines "
                    "target schema for {} stream.".format(stream_name))

            #  Define grantees
            #  ---------------
            #  Grantees can be defined in multiple ways:
            #
            #   1: 'default_target_schema_select_permissions' key  : USAGE and SELECT privileges will be granted on
            #       every table to a given role for every incoming stream if not specified explicitly in the
            #       `schema_mapping` object
            #   2: 'target_schema_select_permissions' key : Roles to grant USAGE and SELECT privileges defined
            #       explicitly for a given stream.
            #           Example config.json:
            #               "schema_mapping": {
            #                   "my_tap_stream_id": {
            #                       "target_schema": "my_vertica_schema",
            #                       "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                   }
            #               }
            self.grantees = self.connection_config.get('default_target_schema_select_permissions')
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.grantees = config_schema_mapping[stream_schema_name].get('target_schema_select_permissions',
                                                                              self.grantees)

            self.data_flattening_max_level = self.connection_config.get('data_flattening_max_level', 0)
            self.flatten_schema = flatten_schema(stream_schema_message['schema'],
                                                 max_level=self.data_flattening_max_level)

    def open_connection(self):
        # TODO: Check for right ssl config.
        """Open Vertica connection"""
        conn_string = dict(
            host=self.connection_config['host'],
            user=self.connection_config['user'],
            port=self.connection_config['port'],
            password=self.connection_config['password'],
            database=self.connection_config['dbname'],
            autocommit=True, # autocommit is off by default
            use_prepared_statements=False, # using server-side prepared statements is disabled by default
            log_path=None   # This will not log from root vertica.
        )

        # SSL is disabled by default
        if 'ssl' in self.connection_config and self.connection_config['ssl'] == 'true':
            conn_string += " sslmode='require'"

        return vertica.connect(**dict(conn_string))

    def query(self, query, params=None):
        """Run a SQL query in vertica"""
        self.logger.debug("Running query: %s", query)
        with self.open_connection() as connection:
            with connection.cursor('dict') as cur:
                cur.execute(query, parameters=params)
                fetchall = format_json(cur.fetchall())
                if cur.rowcount > 0:
                    return fetchall
                return []

    def table_name(self, stream_name, is_temporary=False, without_schema=False):
        """Generate target table name"""
        stream_dict = stream_name_to_dict(stream_name)
        table_name = stream_dict['table_name']
        v_table_name = table_name.replace('.', '_').replace('-', '_').lower()

        if is_temporary:
            return 'tmp_{}'.format(str(uuid.uuid4()).replace('-', '_'))

        if without_schema:
            return f'"{v_table_name.lower()}"'

        return f'{self.schema_name}."{v_table_name.lower()}"'

    def record_primary_key_string(self, record):
        """Generate a unique PK string in the record"""
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        flatten = flatten_record(record, self.flatten_schema, 
                                 max_level=self.data_flattening_max_level)
        try:
            key_props = [str(flatten[p]) for p in self.stream_schema_message['key_properties']]
        except Exception as exc:
            self.logger.info("Cannot find %s primary key(s) in record: %s",
                             self.stream_schema_message['key_properties'],
                             flatten)
            raise exc
        return ','.join(key_props)

    def record_to_csv_line(self, record):
        flatten = flatten_record(record, self.flatten_schema, 
                                 max_level=self.data_flattening_max_level)
        return ','.join(
            [
                json.dumps(flatten[name], ensure_ascii=False)
                if name in flatten and (flatten[name] == 0 or flatten[name]) else ''
                for name in self.flatten_schema
            ]
        )

    def load_csv(self, file, count, size_bytes):
        # TODO: Keep the columns of the csv as fcsvparser require columns inside the file.
        # TODO: Add config for aws key and bucket.
        # TODO: Check if compression is supported.
        """Load CSV files into Vertica database"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        self.logger.info("Loading %d rows into '%s'", count, self.table_name(stream, False))

        with self.open_connection() as connection:
            with connection.cursor('dict') as cur:
                inserts = 0
                updates = 0

                temp_table = self.table_name(stream_schema_message['stream'], is_temporary=True)
                cur.execute(self.create_table_query(table_name=temp_table, is_temporary=True, is_flex=False))

                cur.fetchall()
                copy_sql = ("""COPY {table} FROM STDIN PARSER fcsvparser(delimiter=',', type='traditional') ABORT ON ERROR"""
                            .format(table=temp_table, fields=', '.join(self.column_names()) ))
                with open(file, 'rb') as fs:
                    cur.copy(copy_sql, add_columns(fs.read(), ', '.join(self.column_names())))

                self.logger.info('COPIED TABLE : %s', format_json(cur.execute(f'SELECT * FROM {temp_table}').fetchall()))
                cur.fetchall()
                if len(self.stream_schema_message['key_properties']) > 0:
                    cur.execute(self.update_from_temp_table(temp_table))
                    cur.fetchall()
                    updates = cur.rowcount
                cur.execute(self.insert_from_temp_table(temp_table))
                cur.fetchall()
                inserts = cur.rowcount

                self.logger.info('Loading into %s: %s',
                                 self.table_name(stream, False),
                                 json.dumps({'inserts': inserts, 'updates': updates, 'size_bytes': size_bytes}))
                self.logger.info('UPDATED TABLE : %s', format_json(cur.execute(f'SELECT * FROM {temp_table}').fetchall()))

    # pylint: disable=duplicate-string-formatting-argument
    def insert_from_temp_table(self, temp_table):
        """Insert non-temp table from aa temp table"""
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'])

        if len(stream_schema_message['key_properties']) == 0:
            return """INSERT INTO {} ({}) (SELECT s.* FROM {} s)
                    """.format(
                            table,
                            ', '.join(columns),
                            temp_table)

        return """INSERT INTO {} ({})
                    (SELECT s.* FROM {} s LEFT OUTER JOIN {} t ON {} WHERE {})
                """.format(
                        table,
                        ', '.join(columns),
                        temp_table,
                        table,
                        self.primary_key_condition('t'),
                        self.primary_key_null_condition('t'))

    def update_from_temp_table(self, temp_table):
        """Update non-temp table from aa temp table"""
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'])

        return """UPDATE {table} SET {column_name} FROM {dataset} s WHERE {pk}
                """.format(
                        table=table,
                        column_name=', '.join(['{}=s.{}'.format(c, c) for c in columns]),
                        dataset=temp_table,
                        pk=self.primary_key_condition(table))

    def primary_key_condition(self, right_table):
        # TODO: Merge primary_key_condition funcions.
        """Returns primary keys"""
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['s.{} = {}.{}'.format(c, right_table, c) for c in names])

    def primary_key_null_condition(self, right_table):
        """Returns primary keys with IS NULL condition"""
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['{}.{} is null'.format(right_table, c) for c in names])

    def column_names(self, double_inverted_commas=True):
        """List of all column names"""
        if double_inverted_commas:
            return [safe_column_name(name) for name in self.flatten_schema]
        return [str(name) for name in self.flatten_schema]

    def create_table_query(self, table_name=None, is_temporary=False, is_flex=False):
        """Generate CREATE TABLE SQL"""
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        primary_key = ["PRIMARY KEY ({})".format(', '.join(primary_column_names(stream_schema_message)))] \
            if len(stream_schema_message['key_properties']) > 0 else []

        if not table_name:
            gen_table_name = self.table_name(stream_schema_message['stream'], is_temporary=is_temporary)

        return 'CREATE {}{}TABLE IF NOT EXISTS {} ({}){}'.format(
            'FLEX ' if is_flex else '',
            'TEMPORARY ' if is_temporary else '',
            table_name if table_name else gen_table_name,
            ', '.join(columns + primary_key),
            ' ON COMMIT PRESERVE ROWS' if is_temporary else '',
        )

    def grant_usage_on_schema(self, schema_name, grantee):
        """Grant usage on schema"""
        query = "GRANT USAGE ON SCHEMA {} TO {}".format(schema_name, grantee)
        self.logger.info("Granting USAGE privilege on '%s' schema to '%s'... %s", schema_name, grantee, query)
        self.query(query)

    def grant_select_on_all_tables_in_schema(self, schema_name, grantee):
        """Grant select on all tables in schema"""
        query = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}".format(schema_name, grantee)
        self.logger.info("Granting SELECT ON ALL TABLES privilege on '%s' schema to '%s'... %s",
                         schema_name,
                         grantee,
                         query)
        self.query(query)

    @classmethod
    def grant_privilege(cls, schema, grantees, grant_method):
        """Grant privileges on target schema"""
        if isinstance(grantees, list):
            for grantee in grantees:
                grant_method(schema, grantee)
        elif isinstance(grantees, str):
            grant_method(schema, grantees)

    def create_projection(self, stream, column):
        # TODO: EDIT RESOURCE JSON TO INCLUDE ID FOR SCHEMA.
        # TODO: Secondary indexes are not supported with vertica but 
        # The concept of 'projections' can be used instead of indexes.
        # projections are "CREATE TEXT INDEX txtindex-name ON table-name (hash(id), column-name)"
        # TODO: Check if valid for vertica or can it even support the same functionality?
        table = self.table_name(stream)
        table_without_schema = self.table_name(stream, without_schema=True)
        index_name = 'i_{}_{}'.format(table_without_schema[:30].replace(' ', '').replace('"', ''),
                                      column.replace(',', '_'))
        query = ("CREATE PROJECTION IF NOT EXISTS {projection_name} AS SELECT ({column_name}) FROM {table_name}"
                 .format(projection_name=index_name, table_name=table, column_name=column))
        self.logger.info("Creating projection on '%s' table on '%s' column(s)... %s", table, column, query)
        self.query(query)

    def create_projections(self, stream):
        # TODO: - Secondary indexes are not supported with vertica but 
        # The concept of 'projections' can be used instead of indexes.
        if isinstance(self.indices, list):
            for index in self.indices:
                self.create_projection(stream, index)

    def delete_rows(self, stream):
        # TODO: "RETURN[ING]" is not there in Vertica. Find a workaround.
        # Removed as "RETURNING _sdc_deleted_at"
        """Hard delete rows from target table"""
        table = self.table_name(stream)
        query = "DELETE FROM {} WHERE _sdc_deleted_at IS NOT NULL".format(table)
        self.logger.info("Deleting rows from '%s' table... %s", table, query)
        out = self.query(query)
        if out and 'OUTPUT' in out[0]:
            out = out[0]['OUTPUT']
        self.logger.info("DELETE %s", out)

    def create_schema_if_not_exists(self, table_columns_cache=None):
        """Create target schema if not exists"""
        schema_name = self.schema_name
        schema_rows = 0

        # table_columns_cache is an optional pre-collected list of available objects in vertica
        if table_columns_cache:
            schema_rows = list(filter(lambda x: x['TABLE_SCHEMA'] == schema_name, table_columns_cache))
        # Query realtime if not pre-collected
        else:
            schema_rows = self.query(
                """SELECT LOWER(schema_name) schema_name 
                    FROM v_catalog.schemata 
                    WHERE LOWER(schema_name) = %s""",
                (schema_name.lower(),)
            )

        if len(schema_rows) == 0:
            query = "CREATE SCHEMA IF NOT EXISTS {}".format(schema_name)
            self.logger.info("Schema '%s' does not exist. Creating... %s", schema_name, query)
            self.query(query)
            self.grant_privilege(schema_name, self.grantees, self.grant_usage_on_schema)

    def get_tables(self):
        """Get list of tables of certain schema(s) from vertica metadata"""
        return self.query(
            """SELECT table_name 
                FROM v_catalog.tables 
                WHERE table_schema = %s""",
            (self.schema_name,)
        )

    def get_table_columns(self, table_name):
        """Get list of columns and tables of certain schema(s) from vertica metadata"""
        return self.query(
            """SELECT column_name, data_type
                FROM v_catalog.columns
                WHERE lower(table_name) = %s 
                    AND lower(table_schema) = %s""",
            (table_name.replace("\"", "").lower(), self.schema_name.lower())
        )

    def update_columns(self):
        # TODO: refactor the function.
        """Adds required but not existing columns to the target table according to the schema"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, without_schema=True)
        columns = self.get_table_columns(table_name)
        columns_dict = {column['column_name'].lower(): column for column in columns}

        self.logger.info('Columns DICT: %s', columns_dict)

        columns_to_add = []
        for (name, properties_schema) in self.flatten_schema.items():
            if name.lower() not in columns_dict:
                columns_to_add.append(
                    column_clause(name,properties_schema))

        for column in columns_to_add:
            self.add_column(column, stream)

        columns_to_replace = []
        for (name, properties_schema) in self.flatten_schema.items():
            if (name.lower() in columns_dict and 
                    columns_dict[name.lower()]['data_type'].lower() != column_type(properties_schema).lower()):
                self.logger.info('FOR 2ND LOOP: %s: %s: %s', name, columns_dict[name.lower()]['data_type'], column_type(properties_schema))
                columns_to_replace.append(
                    (safe_column_name(name), column_clause(name, properties_schema)))

        self.logger.info('Columns to replace: %s', columns_to_replace)
        for (column_name, column) in columns_to_replace:
            self.version_column(column_name, stream)
            self.add_column(column, stream)

    def drop_column(self, column_name, stream):
        """Drops column from an existing table"""
        drop_column = "ALTER TABLE {} DROP COLUMN {}".format(self.table_name(stream), column_name)
        self.logger.info('Dropping column: %s', drop_column)
        self.query(drop_column)

    def version_column(self, column_name, stream):
        """Versions a column in an existing table"""
        version_column = "ALTER TABLE {} RENAME COLUMN {} TO \"{}_{}\"".format(self.table_name(stream, False),
                                                                               column_name,
                                                                               column_name.replace("\"", ""),
                                                                               time.strftime("%Y%m%d_%H%M"))
        self.logger.info('Versioning column: %s', version_column)
        self.query(version_column)

    def add_column(self, column, stream):
        """Adds a new column to an existing table"""
        add_column = "ALTER TABLE {} ADD COLUMN {}".format(self.table_name(stream), column)
        self.logger.info('Adding column: %s', add_column)
        self.query(add_column)

    def sync_table(self):
        """Creates or alters the target table according to the schema"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, is_temporary=False, without_schema=True)
        tables = self.get_tables()
        self.logger.info("SYNC TABLE: %s", tables)
        found_tables = [table for table in (tables) if f'"{table["table_name"].lower()}"' == table_name]
        if len(found_tables) == 0:
            query = self.create_table_query()
            self.logger.info("Table '%s' does not exist. Creating... %s", table_name, query)
            self.query(query)

            self.grant_privilege(self.schema_name, self.grantees, self.grant_select_on_all_tables_in_schema)
        else:
            self.logger.info("Table '%s' exists", table_name)
            self.update_columns()

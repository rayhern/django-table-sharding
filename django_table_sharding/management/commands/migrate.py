from django.conf import settings
from django.core.management.commands.migrate import Command as MigrationCommand
from django.apps import apps
from django.db import connection, connections
from django.db.migrations.executor import MigrationExecutor
from datetime import datetime
import re
import traceback

'''

    Custom Management Command for Table Sharding.
    Version 1.3
    By: Ray Hernandez
    
    ManyToManyField, and OneToOneField are not supported.
    
    Sharded models can foreign key to other models, but other models cannot foreign key to sharded tables.
    
'''


class InvalidMigrationException(Exception):
    pass


class Command(MigrationCommand):

    def handle(self, *args, **options):

        # Work out which apps and models have migrations...
        # (taken from original django migrate)
        connection.prepare_database()
        executor = MigrationExecutor(connection, None)
        targets = [key for key in executor.loader.graph.leaf_nodes()]
        plan = executor.migration_plan(targets)

        # Get all models that use the ShardedManager...
        all_models = apps.get_models(include_auto_created=True, include_swapped=True)
        models_with_shard = [str(m.__name__).lower() for m in all_models if hasattr(m.objects, 'shard')]
        if len(models_with_shard) > 0:
            print('\n  Sharded models: %s.\n' % models_with_shard)

        # Go through each migration operation, and see if any models are in our sharded list.
        # If yes, add to queues for migrating shards after migrating source database.
        model_changes = []
        add_unique_togethers = []
        remove_unique_togethers = []
        rename_fields = []
        for p in plan:
            for op in p[0].operations:
                max_length = None
                if hasattr(op, 'model_name'):
                    model_name = op.model_name
                    field_name = op.name
                    default_value = ''
                    if model_name in models_with_shard:
                        # This handles renaming of columns or fields.
                        if hasattr(op, 'old_name'):
                            old_name = op.old_name
                            if hasattr(op, 'new_name'):
                                new_name = op.new_name
                                for m in all_models:
                                    if str(m.__name__).lower() == model_name:
                                        meta = getattr(m, '_meta')
                                        db_table = meta.db_table
                                        rename_fields.append((db_table, old_name, new_name))
                                        break
                            continue
                        elif hasattr(op, 'field'):
                            if op.field.__class__.__name__ == 'ManyToManyField' or \
                                    op.field.__class__.__name__ == 'OneToOneField':
                                print('  %s not supported with table sharding.\n' % op.field.__class__.__name__)
                                continue

                            if hasattr(op.field, 'default'):
                                default_value = op.field.default
                            if hasattr(op.field, 'max_length'):
                                max_length = op.field.max_length

                        for m in all_models:
                            if str(m.__name__).lower() == model_name:
                                meta = getattr(m, '_meta')
                                db_table = meta.db_table
                                model_changes.append((db_table, field_name, default_value, max_length))
                                break
                # Unique together operations
                elif hasattr(op, 'unique_together'):
                    unique_together = op.unique_together
                    model_name = op.name
                    if model_name in models_with_shard:
                        db_table = ''
                        model = None
                        for m in all_models:
                            if str(m.__name__).lower() == model_name:
                                # model = m
                                meta = getattr(m, '_meta')
                                db_table = meta.db_table
                                break
                        if len(unique_together) == 0:
                            # find all fields that have constraint, so we can add index back after removing
                            # unique together.
                            remove_unique_togethers.append((db_table,))
                        else:
                            field_list = []
                            for f in op.unique_together:
                                # fields are a set we must convert them to a list.
                                field_list = list(f)
                            if db_table != '':
                                add_unique_togethers.append((db_table, field_list))

        if len(model_changes) == 0 and len(add_unique_togethers) == 0 and len(remove_unique_togethers) == 0:
            print('  No shard migrations to apply.\n')

        # Run Django supplied migrate command.
        super(Command, self).handle(*args, **options)

        # Go through our sharded model changes, and apply them to all sharded tables, after migration has finished.
        if len(model_changes) > 0:
            print('\nMigrating shards...')
            for change in model_changes:
                self.copy_table_changes(change[0], change[1], change[2], change[3], db=options['database'])
            print('Finished!\n')

        # Add all unique together constraints to sharded tables.
        if len(add_unique_togethers) > 0:
            print('\nMigrating unique together on shards...')
            for change in add_unique_togethers:
                self.copy_unique_together(change[0], change[1], db=options['database'])
            print('Finished!\n')

        if len(remove_unique_togethers) > 0:
            print('\nMigrating remove unique together on shards...')
            for change in remove_unique_togethers:
                self.remove_unique_together(change[0], db=options['database'])
            print('Finished!\n')

        if len(rename_fields) > 0:
            print('\nMigrating field name change on shards...')
            for change in rename_fields:
                self.rename_fields(change[0], change[1], change[2], db=options['database'])
            print('Finished!\n')

    def rename_fields(self, db_table, old_field, new_field, db='default'):
        cursor = connections[db].cursor()
        tables = self.get_sharded_tables(cursor, db_table)

        # Could not find the field name, it must be a foreign key.
        rows = self.run_sql(cursor, "SELECT COLUMN_TYPE, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s';" % (db_table, new_field))
        if len(rows) > 0:
            column_type = rows[0][0]
            for table in tables:
                rows = self.run_sql(cursor, 'ALTER TABLE %s CHANGE %s %s %s;' % (table, old_field, new_field, column_type))

    def copy_table_changes(self, db_table, field_name, default_value, max_length, db='default'):
        """
        Copy all changes from the source db table on field that has changed.
        This will also call compare_indexes().
        """
        # Get all tables that need to be altered
        cursor = connections[db].cursor()

        tables = self.get_sharded_tables(cursor, db_table)

        if len(tables) == 0:
            print('No sharded tables for %s.' % db_table)
            return

        created = False
        dropped = False
        foreign_key = False

        # Get info on field that was altered. If it was dropped, we drop the column from all other tables.
        try:
            rows = self.run_sql(cursor, "SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s';" % (db_table, field_name))
            if len(rows) == 0:
                # Could not find the field name, it must be a foreign key.
                rows = self.run_sql(cursor, "SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s_id';" % (db_table, field_name))
                if len(rows) > 0:
                    field_name = '%s_id' % field_name
                    foreign_key = True

            if len(rows) > 0:
                column_name = rows[0][0]
                data_type = rows[0][1]
                nullable = rows[0][2]

                # Fix: (1681, 'Integer display width is deprecated and will be removed in a future release.')
                # if 'int(' in data_type and data_type[:3] == 'int':
                #     pattern = re.compile(r'[\d\(\)]+')
                #     data_type = pattern.sub('', data_type)

                if nullable == 'YES':
                    for table in tables:
                        # check to make sure column doesn't already exist.
                        rows = self.run_sql(cursor, 'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "%s" AND COLUMN_NAME="%s";' % (table, field_name))
                        if len(rows) == 0:
                            rows = self.run_sql(cursor, 'ALTER TABLE %s ADD COLUMN %s %s;' % (table, column_name, data_type))
                            created = True
                else:
                    if default_value is True:
                        for table in tables:
                            rows = self.run_sql(cursor, 'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "%s" AND COLUMN_NAME="%s";' % (table, field_name))
                            if len(rows) == 0:
                                rows = self.run_sql(cursor, 'ALTER TABLE %s ADD COLUMN %s %s DEFAULT "1";' % (table, column_name, data_type))
                                created = True
                    elif default_value is False:
                        for table in tables:
                            rows = self.run_sql(cursor,'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "%s" AND COLUMN_NAME="%s";' % (table, field_name))
                            if len(rows) == 0:
                                rows = self.run_sql(cursor, 'ALTER TABLE %s ADD COLUMN %s %s DEFAULT "0";' % (table, column_name, data_type))
                                created = True
                    else:
                        for table in tables:
                            rows = self.run_sql(cursor, 'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "%s" AND COLUMN_NAME="%s";' % (table, field_name))
                            if len(rows) == 0:
                                if 'datetime' not in data_type and 'NOT_PROVIDED' not in str(default_value):
                                    # if this is not a datetimefield, set the default value from
                                    # migration operation.
                                    rows = self.run_sql(cursor, 'ALTER TABLE %s ADD COLUMN %s %s DEFAULT "%s";' % (table, column_name, data_type, default_value))
                                else:
                                    # Django will handle auto_add and auto_add_now. MySQL 5.6 does not allow
                                    # setting default value.
                                    rows = self.run_sql(cursor, 'ALTER TABLE %s ADD COLUMN %s %s;' % (table, column_name, data_type))
                                created = True

                # We added a field to our databases, lets see if we added an index from the original mysql table.
                self.compare_indexes(cursor, db_table, tables, field_name)

            else:
                # Field has been dropped from original db, lets drop the field on all shards.
                # Because column doesn't exist in source database, we must check field in both ways,
                # on the first sharded table: field_name, and field_name_id
                rows = self.run_sql(cursor, 'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "%s" AND COLUMN_NAME="%s";' % (tables[0], field_name))
                if len(rows) == 0:
                    rows = self.run_sql(cursor, 'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "%s" AND COLUMN_NAME="%s_id";' % (tables[0], field_name))
                    if len(rows) > 0:
                        field_name = '%s_id' % field_name
                for table in tables:
                    rows = self.run_sql(cursor, "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE COLUMN_NAME='%s' AND REFERENCED_TABLE_NAME IS NOT NULL AND TABLE_NAME='%s';" % (field_name, table))
                    if len(rows) > 0:
                        constraint_name = rows[0][0]
                        rows = self.run_sql(cursor, 'ALTER TABLE %s DROP FOREIGN KEY %s' % (table, constraint_name))
                    rows = self.run_sql(cursor, 'ALTER TABLE %s DROP COLUMN %s;' % (table, field_name))
                dropped = True
        except:
            print(traceback.format_exc())

        # If we got a max_length in the migration operation, lets set it on the model.
        if max_length is not None and created is False and dropped is False:
            rows = self.run_sql(cursor, "SELECT COLUMN_NAME,COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s';" % (db_table, field_name))
            if len(rows) > 0:
                column_name = rows[0][0]
                column_type = rows[0][1]
                for table in tables:
                    rows = self.run_sql(cursor, "ALTER TABLE %s MODIFY %s %s;" % (table, column_name, column_type))

        # If we got a default value set, but that was the only change to the model, lets set the default value.
        if default_value != '' and created is False and dropped is False:
            rows = self.run_sql(cursor, "SELECT COLUMN_NAME,COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s';" % (db_table, field_name))
            if len(rows) > 0:
                column_name = rows[0][0]
                column_type = rows[0][1]
                for table in tables:
                    if default_value is True:
                        default_value = '1'
                    elif default_value is False:
                        default_value = '0'
                    if default_value is not None:
                        if 'datetime' not in column_type and 'NOT_PROVIDED' not in str(default_value):
                            rows = self.run_sql(cursor, "ALTER TABLE %s ALTER %s SET DEFAULT '%s';" % (table, column_name, default_value))
                    else:
                        # default value is null.
                        rows = self.run_sql(cursor, "ALTER TABLE %s MODIFY %s %s;" % (table, column_name, column_type))

        # Handle foreign keys.
        if foreign_key is True and dropped is False:
            # Get all models and match it with our field id.
            models = apps.get_models(include_auto_created=True, include_swapped=True)
            model_name = field_name.replace('_id', '')
            for m in models:
                if model_name == str(m.__name__).lower():
                    meta = getattr(m, '_meta')
                    fk_table = meta.db_table
                    # all_models = [str(m.__name__).lower() for m in all_models if hasattr(m.objects, 'shard')]
                    for table in tables:
                        rows = self.run_sql(cursor, "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE COLUMN_NAME='%s' AND REFERENCED_TABLE_NAME IS NOT NULL AND TABLE_NAME='%s';" % (field_name, table))
                        if len(rows) == 0:
                            rows = self.run_sql(cursor, 'ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s(id);' % (table, field_name, fk_table))
                    break

        # Close the connection when we are finished with entire process.
        try:
            cursor.close()
        except:
            print(traceback.format_exc())

    def compare_indexes(self, cursor, db_table, tables, field_name):
        """
        Compare indexes on main table with that of the shards. If index does not exist, create it.
        (We do not need to pass db parameter here, because we already have a cursor.)
        """
        # First we get all indexes on our old db.
        # SHOW INDEX FROM api_lead WHERE COLUMN_NAME="phone_number" AND KEY_NAME NOT LIKE '%_uniq';
        rows = self.run_sql(cursor, 'SHOW INDEX FROM %s WHERE COLUMN_NAME="%s" AND KEY_NAME NOT LIKE "%%_uniq";' % (db_table, field_name))
        if len(rows) > 0:
            # unique field returned from original table.
            unique_row = rows[0][1]
            for table in tables:
                # Check to make sure the index doesn't already exist.
                rows = self.run_sql(cursor, 'SHOW INDEX FROM %s WHERE COLUMN_NAME="%s" AND KEY_NAME NOT LIKE "%%_uniq";' % (table, field_name))
                if len(rows) > 0:
                    # we have an index to change.
                    rows = self.run_sql(cursor, 'ALTER TABLE %s DROP INDEX %s' % (table, field_name))
                # non unique so 1 is really no index.
                if str(unique_row) == '1':
                    rows = self.run_sql(cursor, 'ALTER TABLE %s ADD INDEX (%s);' % (table, field_name))
                else:
                    rows = self.run_sql(cursor, 'ALTER TABLE %s ADD UNIQUE (%s)' % (table, field_name))
        elif len(rows) == 0:
            # Remove existing index, if we no longer have an index in table.
            for table in tables:
                rows = self.run_sql(cursor, 'SHOW INDEX FROM %s WHERE COLUMN_NAME="%s" AND KEY_NAME NOT LIKE "%%_uniq";' % (table, field_name))
                if len(rows) > 0:
                    rows = self.run_sql(cursor, 'ALTER TABLE %s DROP INDEX %s' % (table, field_name))

    def remove_unique_together(self, db_table, db='default'):
        """
        Removes unique together for all unique together constraints and shards.
        """
        cursor = connections[db].cursor()
        tables = self.get_sharded_tables(cursor, db_table)

        for table in tables:
            rows = self.run_sql(cursor, 'SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = "%s" AND CONSTRAINT_TYPE = "UNIQUE" AND CONSTRAINT_NAME LIKE "%%_uniq";' % table)
            if len(rows) > 0:
                constraint_name = rows[0][0]
                rows = self.run_sql(cursor, 'ALTER TABLE %s DROP INDEX %s' % (table, constraint_name))

        # Close the connection
        try:
            cursor.close()
        except:
            print(traceback.format_exc())

    def copy_unique_together(self, db_table, field_list, db='default'):
        """
        Copies unique together constraint from source database table to all shards.
        """
        cursor = connections[db].cursor()
        index_name = '_'.join(field_list)
        # We add _uniq so it is not confused with other indexes like Django does.
        index_name = '%s_uniq' % index_name

        tables = self.get_sharded_tables(cursor, db_table)

        real_field_list = []
        for field_name in field_list:
            rows = self.run_sql(cursor, "SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s';" % (db_table, field_name))
            real_field = field_name
            if len(rows) == 0:
                # Could not find the field name, it must be a foreign key.
                rows = self.run_sql(cursor, "SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s_id';" % (db_table, field_name))
                if len(rows) > 0:
                    real_field = '%s_id' % field_name
            else:
                real_field = field_name
            real_field_list.append(real_field)

        field_names = ','.join(real_field_list)
        for table in tables:
            rows = self.run_sql(cursor, 'CREATE UNIQUE INDEX %s ON %s(%s);' % (index_name, table, field_names))

        # Close the sql connection.
        try:
            cursor.close()
        except:
            print(traceback.format_exc())

    def get_sharded_tables(self, cursor, db_table):
        tables = []
        try:
            rows = self.run_sql(cursor, 'SHOW TABLES LIKE "%s%%"' % db_table)
            if len(rows) > 0:
                tables = [row[0] for row in rows if row[0] != db_table]
        except:
            print(traceback.format_exc())
        return tables

    def run_sql(self, cursor, sql):
        """
        Execute sql on a given cursor.
        (db does not need to be past, since we are using corresponding cursor.)
        """
        try:
            print('sql> %s' % sql)
            cursor.execute(sql)
            rows = cursor.fetchall()
            return rows
        except:
            print(traceback.format_exc())
        return []

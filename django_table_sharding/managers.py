from django.db import connections
from django.db.models.expressions import Col
from django.db import models
import datetime
import traceback


def chunks(source_list, batch_size):
    """Yield successive n-sized chunks from source_list."""
    for i in range(0, len(source_list), batch_size):
        yield source_list[i:i+batch_size]


class ShardException(Exception):
    pass


# Specific model manager to not only work with sharding, but also to work with migrations.
class ShardManager(models.Manager):

    def shard(self, table_suffix, db='default'):
        """
        Use a shard of the table and set it to the model.
        Usage: Model.objects.shard(1).all()
        """
        meta = getattr(self.model, '_meta')
        meta._db = db
        meta.db_table = '%s_%s_%s' % (
            str(meta.app_label),
            str(self.model.__name__.lower()), table_suffix)
        # Without clearing the concrete cached fields, we cannot switch between shards in a single session.
        for f in meta.concrete_fields:
            f.cached_col = Col(meta.db_table, f)
        return super(ShardManager, self).using(self.db)

    def default(self):
        """
        Use the original table.
        Usage: Model.objects.default().all()
        """
        meta = getattr(self.model, '_meta')
        meta.db_table = '%s_%s' % (
            str(meta.app_label),
            str(self.model.__name__.lower()))
        meta._db = 'default'
        # Without clearing the concrete cached fields, we cannot switch between shards in a single session.
        for f in meta.concrete_fields:
            f.cached_col = Col(meta.db_table, f)
        return super(ShardManager, self).using(self.db)

    def create(self, table_suffix, db='default', **kwargs):
        meta = getattr(self.model, '_meta')
        db_table = '%s_%s_%s' % (
            str(meta.app_label),
            str(self.model.__name__.lower()), table_suffix)

        for k, v in kwargs.items():
            if isinstance(v, datetime.datetime):
                kwargs[k] = v.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(v, datetime.date):
                kwargs[k] = v.strftime('%Y-%m-%d')
            elif isinstance(v, datetime.time):
                kwargs[k] = v.strftime('%H:%M:%S')
            elif isinstance(v, bool):
                if v is True:
                    kwargs[k] = 1
                else:
                    kwargs[k] = 0

        placeholders = ', '.join(['%s'] * len(kwargs.values()))
        columns = ', '.join(kwargs.keys())
        tuple_list = tuple(kwargs.values())

        try:
            with connections[db].cursor() as cursor:
                try:
                    cursor.execute("INSERT IGNORE INTO %s ( %s ) VALUES ( %s )" % (
                        db_table, columns, placeholders), tuple_list)
                except:
                    print(traceback.format_exc())
        except:
            raise ShardException(traceback.format_exc())

    def bulk_create(self, table_suffix, list_of_dicts, batch_size=5, ignore_conflicts=False, db='default'):
        if len(list_of_dicts) == 0:
            raise ShardException('List of dict field values not defined.')

        meta = getattr(self.model, '_meta')
        db_table = '%s_%s_%s' % (
            str(meta.app_label),
            str(self.model.__name__.lower()), table_suffix)

        for dict_fields in list_of_dicts:
            for k, v in dict_fields.items():
                if isinstance(v, datetime.datetime):
                    dict_fields[k] = v.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(v, datetime.date):
                    dict_fields[k] = v.strftime('%Y-%m-%d')
                elif isinstance(v, datetime.time):
                    dict_fields[k] = v.strftime('%H:%M:%S')
                elif isinstance(v, bool):
                    if v is True:
                        dict_fields[k] = 1
                    else:
                        dict_fields[k] = 0

        placeholders = ', '.join(['%s'] * len(list_of_dicts[0]))
        columns = ', '.join(list_of_dicts[0].keys())

        n_chunks = chunks(list_of_dicts, batch_size)
        for chunk in n_chunks:
            tuple_list = [tuple(d.values()) for d in chunk]
            if ignore_conflicts is False:
                with connections[db].cursor() as cursor:
                    try:
                        cursor.executemany("INSERT INTO %s ( %s ) VALUES ( %s )" % (
                            db_table, columns, placeholders), tuple_list)
                    except:
                        raise ShardException(traceback.format_exc())
            else:
                with connections[db].cursor() as cursor:
                    try:
                        cursor.executemany("INSERT IGNORE INTO %s ( %s ) VALUES ( %s )" % (
                            db_table, columns, placeholders), tuple_list)
                    except:
                        raise ShardException(traceback.format_exc())

    def shard_exists(self, table_suffix, db='default'):
        """
        Check if sharded table exists.
        """
        meta = getattr(self.model, '_meta')
        db_table = '%s_%s_%s' % (
            str(meta.app_label),
            str(self.model.__name__.lower()), table_suffix)
        try:
            with connections[db].cursor() as cursor:
                cursor.execute('SHOW TABLES LIKE "%s%%"' % db_table)
                rows = cursor.fetchall()
                if len(rows) > 0:
                    return True
        except:
            pass
        return False

    @staticmethod
    def copy_table(source_table, destination_table, db='default'):
        """
        Copy original table to new sharded table. Keeps all indexes and unique together.
        """
        try:
            with connections[db].cursor() as cursor:
                cursor.execute('CREATE TABLE IF NOT EXISTS %s LIKE %s;' % (destination_table, source_table))
        except:
            pass


class ShardedModel(models.Model):
    def save(self, *args, **kwargs):
        if len(args) > 0:
            shard = args[0]
            db_table = '%s_%s_%s' % (self._meta.app_label, str(self._meta.model.__name__).lower(), shard)
            self._meta.db_table = db_table
            for f in self._meta.concrete_fields:
                f.cached_col = Col(self._meta.db_table, f)
            return super(ShardedModel, self).save(**kwargs)
        else:
            raise ShardException('No shard/table suffix specified in save.')

    class Meta:
        abstract = True


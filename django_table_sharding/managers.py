from django.db import connections
from django.db import models
from .utils import copy_model, chunks
import random
import datetime
import traceback
import threading
import multiprocessing


class ShardException(Exception):
    pass


THREAD_LOCK = threading.Lock()
FORK_LOCK = multiprocessing.Lock()


# Specific model manager to not only work with sharding, but also to work with migrations.
class ShardManager(models.Manager):

    def shard(self, table_suffix, db='default'):
        """
        Use a shard of the table and set it to the model.
        Usage: Model.objects.shard(1).all()
        """
        global THREAD_LOCK, FORK_LOCK
        meta = getattr(self.model, '_meta')
        db_table = '%s_%s_%s' % (
            str(meta.app_label),
            str(self.model.__name__.lower()), table_suffix)
        model_name = 'ShardedModel-%s' % random.randint(999999999, 9999999999999999)
        with THREAD_LOCK and FORK_LOCK:
            self.model = copy_model(
                model_name,
                self.model,
                db_table,
                options={'db_table': db_table, 'auto_created': False}
            )
        return super(ShardManager, self).using(db)

    def create(self, table_suffix, db='default', **kwargs):
        meta = getattr(self.model, '_meta')
        db_table = '%s_%s_%s' % (
            str(meta.app_label),
            str(self.model.__name__.lower()), table_suffix)

        delete_keys = []
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
            elif v is None:
                delete_keys.append(k)

        for delete in delete_keys:
            del kwargs[delete]

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

    def bulk_create(self, table_suffix, list_of_dicts, batch_size=1, ignore_conflicts=False, db='default'):
        if len(list_of_dicts) == 0:
            raise ShardException('List of dict field values not defined.')

        meta = getattr(self.model, '_meta')
        db_table = '%s_%s_%s' % (
            str(meta.app_label),
            str(self.model.__name__.lower()), table_suffix)

        for dict_fields in list_of_dicts:
            delete_keys = []
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
                elif v is None:
                    delete_keys.append(k)
            for delete in delete_keys:
                del dict_fields[delete]

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

    objects = ShardManager()

    class Meta:
        abstract = True

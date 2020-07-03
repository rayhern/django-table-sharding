from django.db import models, connections
from django.db.models.expressions import Col
from django.db import models


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
        if 'shard' in kwargs:
            shard = kwargs.pop('shard')
            db_table = '%s_%s_%s' % (self._meta.app_label, self._meta.verbose_name, shard)
            self._meta.db_table = db_table
            for f in self._meta.concrete_fields:
                f.cached_col = Col(self._meta.db_table, f)
            super(ShardedModel, self).save(*args, **kwargs)
        else:
            raise ShardException('No shard specified in save.')

    class Meta:
        abstract = True


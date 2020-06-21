=====================
Django-Table-Sharding
=====================

Django table sharding is an app that will allow you to shard your database tables in the
same database using a shard key or shard suffix.

Detailed documentation is in the "docs" directory.

Quick start
-----------

1. Add "django_table_sharding" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...
        'django_table_sharding',
    ]

Every time migrations are run it will copy changes from the source
table to the sharded tables.

Set the model manager to the models you want to shard into their own tables.
Example::

    objects = ShardManager()

The migration process will automatically find all sharded models and apply the
new migrations.

Sharded models can have ForeignKeys, but other models cannot foreign key to the sharded
models because original table is not used.

Sharded models cannot contain OneToOneField, or ManyToManyField, for same reason.

When running migrations the normal migration will run as normal, and then changes to source table,
will be applied to all shards in the database.

Before deploying to production please make sure everything fits your needs.



django-table-sharding
---------------------

Django table sharding is an app that will allow you to shard your database tables in the
same database using a shard key or shard suffix.

Quick start
-----------

`pip install django-table-sharding`

Add "django_table_sharding" to your INSTALLED_APPS.

Every time migrations are run it will copy changes from the source
table to the sharded tables.

Set the model manager to the models you want to shard into separate tables.
Example:

    objects = ShardManager()

The migration process will automatically find all sharded models and apply the
new migrations.

Sharded models can have ForeignKeys, but other models cannot foreign key to the sharded
models because original table is not used.

Sharded models cannot contain OneToOneField, or ManyToManyField, for same reason.

When running migrations the normal migration will run as normal, and then changes to source table,
will be applied to all shards in the database.

You also must declare your sharded models in this way...ShardedModel inherits from models.Model.

    from django_table_sharding.managers import ShardedModel
    
    class Person(ShardedModel):
        etc...

Before deploying to production please make sure everything fits your needs.

Example Usage
-------------

`from django_table_sharding.managers import ShardManager`
- Import the ShardManager model manager for Django models.

`Person.objects.shard(1).all()`    
- Shows all people from first shard suffix.

`Person.objects.copy_table('api_person', 'api_person_1')`
- To create a new shard from existing source database.

`python manage.py migrate`
- Run migration command as normal.

`Person.objects.shard_exists(5)`
- Returns True/False if (shard) exists in the database. If it doesn't exist, you can create it with copy_table().

`person.save(1)`
- Saves a model instance to the specified shard.

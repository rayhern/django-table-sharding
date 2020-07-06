from django.db import models
from django.apps.registry import apps
from django.db.models.expressions import Col


def chunks(source_list, batch_size):
    """Yield successive n-sized chunks from source_list."""
    for i in range(0, len(source_list), batch_size):
        yield source_list[i:i+batch_size]


class ModelRegistry:
    """
    For removing temporary models created by sharding.
    """
    def __init__(self, app_label):
        self.app_label = app_label

    def is_registered(self, model_name):
        return model_name.lower() in apps.all_models[self.app_label]

    def get_model(self, model_name):
        try:
            return apps.get_model(self.app_label, model_name)
        except LookupError:
            return None

    def unregister_model(self, model_name):
        try:
            del apps.all_models[self.app_label][model_name.lower()]
        except KeyError as err:
            raise LookupError("'{}' not found.".format(model_name)) from err


def copy_model(name, model_to_copy, db_table, options=None):
    """
    Deep copy a model's fields and database attributes, so that we don't modify the
    original models table.

    """
    copy_meta = getattr(model_to_copy, '_meta')
    fields = copy_meta.fields
    app_label = copy_meta.app_label
    module = model_to_copy.__module__

    class Meta:
        # Using type('Meta', ...) gives a dictproxy error during model creation
        pass

    # app_label must be set using the Meta inner class
    if app_label:
        setattr(Meta, 'app_label', app_label)

    # Update Meta with any options that were provided
    if options is not None:
        for key, value in options.items():
            setattr(Meta, key, value)

    # Set up a dictionary to simulate declarations within a class
    # Create the class, which automatically triggers ModelBase processing
    attrs = dict()
    attrs['__module__'] = module
    attrs['Meta'] = Meta

    # Prepare to copy all fields from existing model.
    if fields:
        names = types = []
        copy_meta = getattr(model_to_copy, '_meta')
        for item in copy_meta.concrete_fields:
            names.append(item.name)
            types.append(item)
        field_dict = dict(zip(names, types))
        attrs.update(field_dict)

    model = type(name, (models.Model,), attrs)

    # Remove from model registry immediately so it doesn't complain about us changing the model.
    ModelRegistry(app_label).unregister_model(name)

    new_meta = getattr(model, '_meta')
    for f in new_meta.concrete_fields:
        f.cached_col = Col(db_table, f)

    return model

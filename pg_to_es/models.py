import inspect
import functools
import datetime
import iso8601
from pg_to_es import misc



def autoargs(*include, **kwargs):
    def _autoargs(func):
        spec = inspect.getfullargspec(func)
        attrs = spec.args
        varargs = spec.varargs
        defaults = spec.defaults

        def sieve(attr):
            if kwargs and attr in kwargs['exclude']:
                return False
            if not include or attr in include:
                return True
            else:
                return False

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # handle default values
            # import pdb;pdb.set_trace()
            if defaults:
                for attr, val in zip(reversed(attrs), reversed(defaults)):
                    if sieve(attr):
                        setattr(self, attr, val)
                        # but 0 is the meaning something
                        # we only think None can be remove
                        # or just ignore
                        if val is not None:
                            self[attr] = val
            # handle positional arguments
            positional_attrs = attrs[1:]
            for attr, val in zip(positional_attrs, args):
                if sieve(attr):
                    setattr(self, attr, val)
                    if val is not None:
                        self[attr] = val
            # handle varargs
            if varargs:
                remaining_args = args[len(positional_attrs):]
                if sieve(varargs):
                    setattr(self, varargs, remaining_args)

            # handle varkw
            if kwargs:
                for attr, val in kwargs.items():
                    if sieve(attr):
                        setattr(self, attr, val)
                        if val is not None:
                            self[attr] = val
            return func(self, *args, **kwargs)

        return wrapper

    return _autoargs


class INFO(dict):
    columns = []

    @autoargs()
    def __init__(self, **kwargs):
        super(INFO, self).__init__()
        update_time = kwargs.get("update_time")
        create_time = kwargs.get("create_time")
        if update_time:
            update_time = iso8601.parse_date(update_time)
        else:
            update_time = datetime.datetime.utcnow()

        if create_time:
            create_time = iso8601.parse_date(create_time)
        else:
            create_time = datetime.datetime.utcnow()

        self["update_time"] = int(1000 * update_time.timestamp())
        self["create_time"] = int(1000 * create_time.timestamp())
        # self["@timestamp"] = int(1000 * update_time.timestamp())
        self.has_extra_info = False

    @classmethod
    def same_value(cls, before, after):
        for key_care in cls.columns:
            # sometimes the before don't have total value
            # even I do alter table xxx replica identity full;
            if before.get(key_care, None) != after.get(key_care, None):
                return False
        return True

    @classmethod
    def changed_value(cls, before, after):
        changed_info = {}
        for key_care in cls.columns:
            if key_care not in after:
                continue
            if before.get(key_care, None) != after.get(key_care, None):
                changed_info[key_care] = after[key_care]
                if key_care in ["update_time"]:
                    tmp = iso8601.parse_date(changed_info[key_care])
                    changed_info[key_care] = int(1000 * tmp.timestamp())
                if key_care in ["description"]:
                    changed_info["description_trigger"] = 1
            else:
                if key_care in ["description"]:
                    changed_info["description_trigger"] = 0
        return changed_info


class General(INFO):
    """ some table don't have id or name but general it have"""
    columns = []

    def __init__(self, **kwargs):
        super(General, self).__init__(**kwargs)
        self["id"] = kwargs.get("id")




class Album(General):
    columns = [
        "id", "simple_id", "name", "cover_url", "description",
        "episode_count", "user_id", "album_type", "status", "cover_color",
        "origin_author_name", "score", "voice_gender", "update_time",
        "series", "series_part"
    ]

    def __init__(self, **kwargs):
        super(General, self).__init__(**kwargs)
        # add author_name
        # self.has_extra_info = True

mapping = {
    "album": Album,
}

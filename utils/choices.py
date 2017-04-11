from enum import Enum, EnumMeta


class ChoicesEnumMeta(EnumMeta):
    def __new__(mcs, cls, bases, classdict):
        labels = {}
        orig_members = classdict._member_names.copy()
        for key in orig_members:
            if isinstance(classdict[key], (list, tuple)) and not key.startswith('_'):
                cpy = classdict[key]
                del classdict[key]
                classdict._member_names.remove(key)
                classdict[key], labels[key] = cpy
        classdict._member_names = orig_members
        classdict['_choice_labels'] = property(lambda x: labels)
        return super().__new__(mcs, cls, bases, classdict)

    def _create_(cls, class_name, names=None, *args, **kwargs):
        labels = {}

        def prepare_item(index, key, value=None, label=None):
            if label:
                labels[key] = label
            value = value if value is not None else index
            return key, value

        if isinstance(names, (tuple, list)):
            names = [prepare_item(i, *row) for i, row in enumerate(names, 1)]

        klass = super()._create_(class_name, names, *args, **kwargs)
        setattr(klass, '_choice_labels', property(lambda x: labels))
        return klass


class ChoicesEnum(Enum, metaclass=ChoicesEnumMeta):
    @property
    def label(self):
        return self._choice_labels.get(self.name, self.name)

    @classmethod
    def choices(cls):
        return tuple((c.value, c.label) for c in cls)

    @classmethod
    def values(cls):
        return list(c.value for c in cls)

    @classmethod
    def labels(cls):
        return list(c.label for c in cls)

    def __int__(self):
        return int(self.value)

    def __hash__(self):
        return self.value

    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        return self.value == other

    def __gt__(self, other):
        return self.value > other

    def __lt__(self, other):
        return self.value < other

    def __ge__(self, other):
        return self.value >= other

    def __le__(self, other):
        return self.value <= other

    def deconstruct(self):
        return (
            '%s.%s' % (self.__class__.__module__, self.__class__.__name__),
            (self.value,),
            {}
        )

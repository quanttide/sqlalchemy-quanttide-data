from collections import OrderedDict
from inspect import isclass

import tablib

class Record(object):
    """A row, from a query, from a database."""
    __slots__ = ('_keys', '_values')

    def __init__(self, keys, values):
        self._keys = keys
        self._values = values

        # Ensure that lengths match properly.
        assert len(self._keys) == len(self._values)

    def keys(self):
        """Returns the list of column names from the query."""
        return self._keys

    def values(self):
        """Returns the list of values from the query."""
        return self._values

    def __repr__(self):
        return '<Record {}>'.format(self.export('json')[1:-1])

    def __getitem__(self, key):
        # Support for index-based lookup.
        if isinstance(key, int):
            return self.values()[key]

        # Support for string-based lookup.
        if key in self.keys():
            i = self.keys().index(key)
            if self.keys().count(key) > 1:
                raise KeyError("Record contains multiple '{}' fields.".format(key))
            return self.values()[i]

        raise KeyError("Record contains no '{}' field.".format(key))

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __dir__(self):
        standard = dir(super(Record, self))
        # Merge standard attrs with generated ones (from column names).
        return sorted(standard + [str(k) for k in self.keys()])

    def get(self, key, default=None):
        """Returns the value for a given key, or default."""
        try:
            return self[key]
        except KeyError:
            return default

    def as_dict(self, ordered=False):
        """Returns the row as a dictionary, as ordered."""
        items = zip(self.keys(), self.values())

        return OrderedDict(items) if ordered else dict(items)

    @property
    def dataset(self):
        """A Tablib Dataset containing the row."""
        data = tablib.Dataset()
        data.headers = self.keys()

        row = _reduce_datetimes(self.values())
        data.append(row)

        return data

    def export(self, format, **kwargs):
        """Exports the row to the given format."""
        return self.dataset.export(format, **kwargs)


class RecordCollection(list):
    """A set of excellent Records from a query."""

    def export(self, format, **kwargs):
        """Export the RecordCollection to a given format (courtesy of Tablib)."""
        return self.dataset.export(format, **kwargs)

    @property
    def dataset(self):
        """A Tablib Dataset representation of the RecordCollection."""
        # Create a new Tablib Dataset.
        data = tablib.Dataset()

        # If the RecordCollection is empty, just return the empty set
        # Check number of rows by typecasting to list
        if len(self) == 0:
            return data

        # Set the column names as headers on Tablib Dataset.
        first = self[0]

        data.headers = first.keys()
        for row in self.all():
            row = _reduce_datetimes(row.values())
            data.append(row)

        return data

    def all(self, as_dict=False, as_ordereddict=False):
        if as_dict:
            return [r.as_dict() for r in self]
        elif as_ordereddict:
            return [r.as_dict(ordered=True) for r in self]

        return self

    def as_dict(self, ordered=False):
        return self.all(as_dict=not ordered, as_ordereddict=ordered)

    def first(self, default=None, as_dict=False, as_ordereddict=False):
        """Returns a single record for the RecordCollection, or `default`. If
        `default` is an instance or subclass of Exception, then raise it
        instead of returning it."""

        # Try to get a record, or return/raise default.
        try:
            record = self[0]
        except IndexError:
            if isexception(default):
                raise default
            return default

        # Cast and return.
        if as_dict:
            return record.as_dict()
        elif as_ordereddict:
            return record.as_dict(ordered=True)
        else:
            return record

    def one(self, default=None, as_dict=False, as_ordereddict=False):
        """Returns a single record for the RecordCollection, ensuring that it
        is the only record, or returns `default`. If `default` is an instance
        or subclass of Exception, then raise it instead of returning it."""

        # Ensure that we don't have more than one row.
        try:
            self[1]
        except IndexError:
            return self.first(default=default, as_dict=as_dict, as_ordereddict=as_ordereddict)
        else:
            raise ValueError('RecordCollection contained more than one row. '
                             'Expects only one row when using '
                             'RecordCollection.one')

    def scalar(self, default=None):
        """Returns the first column of the first row, or `default`."""
        row = self.one()
        return row[0] if row else default


def isexception(obj):
    """Given an object, return a boolean indicating whether it is an instance
    or subclass of :py:class:`Exception`.
    """
    if isinstance(obj, Exception):
        return True
    if isclass(obj) and issubclass(obj, Exception):
        return True
    return False


def _reduce_datetimes(row):
    """Receives a row, converts datetimes to strings."""

    row = list(row)

    for i in range(len(row)):
        if hasattr(row[i], 'isoformat'):
            row[i] = row[i].isoformat()
    return tuple(row)

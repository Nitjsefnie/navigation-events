"""Explicit read schemas for the five source files.

Declared rather than inferred, for three reasons:

* inference costs an extra full pass over the bzip2-compressed input;
* inference types a field from the values it happens to see:
  ``relativeLayoutSize.width`` only ever holds ``1`` or ``-1`` in this
  extract and would become a long, so a later file with ``0.5`` would change
  the schema of the job;
* together with ``mode=FAILFAST`` in the loader, a declared schema turns a
  value of the wrong type into a failed job instead of a silent null, which
  is what a monitoring job should do. It does not catch a key that goes
  missing (it reads as null; ``loader.check_required_fields`` covers the
  keys the measures need) or a new key (the schema ignores it).

Every field observed in a full scan of the files is declared, including the
payload fields no task reads, so the schemas document the data contract.
``eventTime`` is read as a string and parsed in the loader (see there).
"""

from pyspark.sql import types as T

# No field is declared non-nullable: Spark file sources make every column
# nullable on read, so the flag would promise a check nobody performs.
_EVENT_TIME = T.StructField("eventTime", T.StringType())
_NAVIGATION = T.StructField("navigation", T.LongType())
_SERIAL_ID = T.StructField("serialId", T.LongType())
_RELATIVE_TIME = T.StructField("relativeTimeMs", T.LongType())
# boxIndex reaches 2.4e15 on dynamically created boxes: it needs a long.
_BOX_INDEX = T.StructField("boxIndex", T.LongType())


def _pair(name: str, first: str, second: str, data_type=T.DoubleType()) -> T.StructField:
    return T.StructField(
        name, T.StructType([T.StructField(first, data_type), T.StructField(second, data_type)])
    )


REQUEST = T.StructType([_EVENT_TIME, T.StructField("hwType", T.StringType()), _NAVIGATION])

PAGE_CHANGE = T.StructType([
    _EVENT_TIME, _NAVIGATION, _SERIAL_ID, _RELATIVE_TIME,
    # Viewport in pixels: on every serial-0 row (the initial render of the
    # results page) and on later rows that report a resize.
    _pair("clientSize", "height", "width", T.LongType()),
    # Page layout relative to the viewport; -1 appears as a sentinel value.
    _pair("relativeLayoutSize", "height", "width"),
])

BOX_CREATE = T.StructType([
    _EVENT_TIME, _NAVIGATION, _SERIAL_ID, _RELATIVE_TIME,
    _BOX_INDEX,
    T.StructField("checkVisibility", T.BooleanType()),
])

BOX_CHANGE = T.StructType([
    _EVENT_TIME, _NAVIGATION, _SERIAL_ID, _RELATIVE_TIME,
    _BOX_INDEX,
    # Position and size are absent on about a third of the rows: only the
    # attributes that changed seem to be sent.
    _pair("relativePosition", "x", "y"),
    _pair("relativeSize", "height", "width"),
    T.StructField("visibility", T.BooleanType()),
])

MOUSE_DOWN = T.StructType([
    _EVENT_TIME, _NAVIGATION, _SERIAL_ID, _RELATIVE_TIME,
    _BOX_INDEX,
    # Either true or absent, never false.
    T.StructField("activeElement", T.BooleanType()),
])

# File stem -> schema of the four client-side (browser) streams. The stem is
# also the value of the ``event_type`` column, so results use the same names
# as the files and the assignment.
CLIENT_EVENT_SCHEMAS = {
    "page-change": PAGE_CHANGE,
    "box-create": BOX_CREATE,
    "box-change": BOX_CHANGE,
    "mouse-down": MOUSE_DOWN,
}

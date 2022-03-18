<!-- markdownlint-disable -->

<a href="..\CaptureFile\CaptureFile.py#L32"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>class</kbd> `CaptureFile`
The CaptureFile constructor opens and returns a capture file named `file_name` for reading or writing, depending on the value of `to_write`. 

If the capture file does not already exist and it is opened for write, or if `force_new_empty_file` is True, then a new file will be created and the initial value for its metadata will be the passed `initial_metadata`. These are the only cases where the passed `initial_metadata` is used, and it is provided as a way of optionally ensuring that a capture file always has metadata even when it is first created. 

The `encoding` argument is used to decode records that are returned. The default is `utf8`, which means the binary records stored in the capture file will be decoded into strings using the utf8 encoding before being returned. If `encoding=None` is set, then the raw bytes will be returned. All of the encodings available at https://docs.python.org/3/library/codecs.html#standard-encodings are valid. 

Only one process can open a capture file for writing at a time. Multiple processes can open the same capture file for read simultaneously with each other and with one process that opens it for write. 

An `InvalidCaptureFile` exception is raised if this constructor is used to open a file that is not a valid capture file, is in an unsupported version of the capture file format, or is a corruptted. 

<a href="..\<string>"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    file_name: str,
    to_write: InitVar[bool] = False,
    initial_metadata: InitVar[Optional[bytes]] = None,
    force_new_empty_file: InitVar[bool] = False,
    encoding: Optional[str] = 'utf_8'
) → None
```








---

<a href="..\CaptureFile\CaptureFile.py#L783"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>method</kbd> `add_record`

```python
add_record(record: Union[str, bytes]) → int
```

Adds the passed `record` to this capture file without committing it and returns the new record count. 

All records added to a capture file are binary. If a string is passed to `add_record`, it will automatically be encoded to binary before being stored. 

If this capture file is not open, then this method will raise a `CaptureFileNotOpen` exception. 

If the capture file is open for read but not for write, then it will raise a `CaptureFileNotOpenForWrite` exception. 

---

<a href="..\CaptureFile\CaptureFile.py#L167"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>method</kbd> `close`

```python
close()
```

Closes the OS file and clears the reference to it. 

All uncommitted records and metadata will be lost. 

If this capture file is already closed, then this call does nothing. 

---

<a href="..\CaptureFile\CaptureFile.py#L827"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>method</kbd> `commit`

```python
commit()
```

Commits records added to the capture file and any metadata that was set since the last commit or, if there was no previous commit, since this capture file was opened for write. 

No records added or metadata that was set will be persistent or visible to other processes until committed by this method. 

Either all records and metadata will be committed, or, in the case of failure, no records or metadata will be committed. 

If this capture file is not open, then this method will raise a `CaptureFileNotOpen` exception. 

If it is not open for write then this method will raise a `CaptureFileNotOpenForWrite` exception. 

---

<a href="..\CaptureFile\CaptureFile.py#L433"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>method</kbd> `get_metadata`

```python
get_metadata() → Optional[bytes]
```

Returns the binary metadata that was stored in the capture file on creation or using `set_metadata`. 

If there is no metadata set, then None is returned. 

If this capture file is not open, then this method will raise a `CaptureFileNotOpen` exception. 

---

<a href="..\CaptureFile\CaptureFile.py#L112"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>method</kbd> `open`

```python
open(to_write: bool = False)
```

Opens this CaptureFile instance for reading or writing, depending on the value of `to_write`. 

By default, this CaptureFile instance is opened for read unless `to_write=True`, in which case it will be opened for write. 

`open` is typically used to reopen a capture file object that was previously closed, but it is also called by the constructor. 

Only one instance of CaptureFile, either within or across processes, can open a capture file for writing at a time. Multiple instances across one more more processes can open the same capture file for read simultaneously with each other and with one instance that opens it for write. 

`open` cannot be called on an instance of CaptureFile that was already opened, although it can be closed and then opened again, potentially with a different to_write flag. 

If any of these conditions are violated, then then this method will raise a `CaptureFileAlreadyOpen` exception. 

---

<a href="..\CaptureFile\CaptureFile.py#L618"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>method</kbd> `record_at`

```python
record_at(record_number: int) → Union[str, bytes]
```

Returns the record stored at the passed `record_number`. 

The first record in the file is at `record_number` = 1. 

If the CaptureFile was opened with `encoding=None`, then the returned value will be the raw bytes of the record. Otherwise, the returned value will be a string created from the binary data using the encoding specified when the constructor was called. 

An attempt to get a record before the first record or beyond the last record available will raise an `IndexError` exception. 

If this capture file is not open, then this method will raise a `CaptureFileNotOpen` exception. 

---

<a href="..\CaptureFile\CaptureFile.py#L673"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>method</kbd> `record_count`

```python
record_count() → int
```

Returns the number of records available when the file was opened or last refreshed. If opened for write, the record count is up-to-date with any records that have been added even if they have not been committed. 

---

<a href="..\CaptureFile\CaptureFile.py#L516"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>method</kbd> `record_generator`

```python
record_generator(
    starting_record_number: int = 1
) → Generator[Union[str, bytes], NoneType, NoneType]
```

Returns a generator of records begining at `starting_record_number` that continues until the end of the file as it existed when `record_generator` was called. 

This is used internally by `__iter__` to provide all the standard iteration capabilities on capture file starting at record 1. 

This is also used internally by `__getitem__` to efficiently support contiguous ranges; e.g. to return the records from 999 to 1010 on a capture file cf use: cf[999:1011]. Non-contiguous ranges are supported by `record_at`. 

If this capture file is not open, then this method will raise a `CaptureFileNotOpen` exception. 

---

<a href="..\CaptureFile\CaptureFile.py#L307"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>method</kbd> `refresh`

```python
refresh()
```

Updates the internal structures of this capture file object to reflect the current state of the file on disk. 

Use `refresh` to allow fetching of records that were added after the capture file was opened for read. If the capture file was opened for write, then no other process could have added records so refreshing is not required. 

If this capture file is not open, then this method will raise a `CaptureFileNotOpen` exception. 

---

<a href="..\CaptureFile\CaptureFile.py#L456"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>method</kbd> `set_metadata`

```python
set_metadata(new_metadata: Optional[bytes])
```

Stores binary data in this capture file that can be retrieved with `get_metadata`. Metadata does not appear as a record, but is instead associated with the capture file as a whole. 

Since `new_metadata` is saved transactionally with records, it can be used to remember where processing left off at the last successful commit. This makes the knowledge of exactly where to restart processing available for recovery after a failure. 

The binary contents of `new_metadata` are completely up to the user; the capture file only sets and gets this data. 

To clear the metadata, `None` can be passed in for `new_metadata`. 

If this capture file is not open, then this method will raise a `CaptureFileNotOpen` exception. 

If it is not open for write then it will raise a `CaptureFileNotOpenForWrite` exception. 


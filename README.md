# CaptureFile - Transactional record logging library

## Overview

Capture files are compressed transactional record logs and by convention use the
extension ".capture". Records can be appended but not modified and are
explicitly committed to the file.

Any records that are added but not committed will not be visible to other
processes and will be lost if the process that added them stops or otherwise
closes the capture file before committing. All records that were added between
commits either become available together or, if the commit fails, are discarded
together. This is true even if the file buffers were flushed to disk as the
number of records added between commits grew.

Records in a capture file are each of arbitrary length and can contain up to 4GB
(2³² bytes) of binary data.

Capture files can quickly retrieve any record by its sequential record number.
This is true even with trillions of records.

Metadata can be attached to and read from a capture file. The current metadata
reference is replaced by any subsequent metadata writes. A metadata update is
also transactional and will be committed together with any records that were
added between commits.

Concurrent read access is supported for multiple threads and OS processes.

Only one writer is permitted at a time.

This is a pure Python implementation with no dependencies beyond the Python
standard library. Development build dependencies are listed in requirements.txt.

Click here for the implementation language independent [internal details of the
design and the data structures
used](https://github.com/MIOsoft/CaptureFile-Python/blob/master/docs/DESIGN.md).

Click here for a detailed description of the [Python CaptureFile
API](https://github.com/MIOsoft/CaptureFile-Python/blob/master/docs/CaptureFile.CaptureFile.md).
The detailed description covers several useful APIs and parameters that are not
covered in the Quickstart below.

To work with capture files visually, you can use the free [MIObdt](https://miosoft.com/miobdt/) application.

## Install

```
pip install CaptureFile
```

## Quickstart

### Example 1. Creating a new capture file and then adding and committing some records to it.


```python
from CaptureFile import CaptureFile

# in the **existing** sibling "TempTestFiles" folder create a new empty capture file
cf = CaptureFile("../TempTestFiles/Test.capture", to_write=True, force_new_empty_file=True)

# add five records to the capture file
cf.add_record("Hey this is my record 1")
cf.add_record("Hey this is my record 2")
cf.add_record("Hey this is my record 3")
cf.add_record("Hey this is my record 4")
cf.add_record("Hey this is my record 5")

# commit records to capture file
cf.commit()

print(f"There are {cf.record_count()} records in this capture file.")

# close the capture file
cf.close()
```

    There are 5 records in this capture file.
    

### Example 2. Reading a record from the capture file created above.


```python
from CaptureFile import CaptureFile

# open existing capture file for reading
cf = CaptureFile("../TempTestFiles/Test.capture")

# retrieve the second record from the capture file
record = cf.record_at(2)
print(record)

# close the capture file
cf.close()
```

    Hey this is my record 2
    

### Example 3. Opening an existing capture file and then reading a range of records from it.


```python
from CaptureFile import CaptureFile

# open existing capture file for reading
cf = CaptureFile("../TempTestFiles/Test.capture")

# retrieve and print records 2 to 3
print(cf[2:4])

# close the capture file
cf.close()
```

    ['Hey this is my record 2', 'Hey this is my record 3']
    

### Example 4. Opening an existing capture file using a context manager and then iterating over all records from it.


```python
from CaptureFile import CaptureFile

# open existing capture file for reading using a context manager
# so no need to close the capture file
with CaptureFile("../TempTestFiles/Test.capture") as cf:

    #iterate over and print all records
    for record in iter(cf):
        print(record)
```

    Hey this is my record 1
    Hey this is my record 2
    Hey this is my record 3
    Hey this is my record 4
    Hey this is my record 5
    

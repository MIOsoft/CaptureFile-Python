
print("Start importing")
from CaptureFile import CaptureFile
print("Done importing")
import time

file_name_1 = R"TempTestFiles/new_capture_file_py.capture"

start = time.time()
number_of_records = 10_000_000
cf = CaptureFile(
    file_name_1,
    to_write=True,
    initial_metadata=b"This is the initial metadata",
    force_new_empty_file=True,
)
for i in range(1, number_of_records + 1):
    cf.add_record(f"Hey this is my record {i:,}")
    if i % 1_000 == 0:
        cf.commit()
cf.commit()
assert cf.record_count() == number_of_records
end = time.time()
print(f"There are {cf.record_count():,} records in this file")
cf.close()
print(f"Elapsed time writing {number_of_records:,} records = {end-start}s")

start = time.time()
cf = CaptureFile(file_name_1)
for i in range(1, number_of_records + 1):
    record = cf.record_at(i)
cf.close()
end = time.time()
print(
    f"Elapsed time reading and validating {number_of_records:,} records using record_at"
    f" = {end-start}s"
)
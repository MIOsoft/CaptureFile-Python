import logging
import time

from multiprocessing import Process
from pathlib import Path
from threading import Thread

from CaptureFile import CaptureFile, CaptureFileAlreadyOpen

LOGGER = logging.getLogger()
file_name_1 = R"TempTestFiles/new_capture_file_py.capture"
file_name_2 = R"TempTestFiles/add_after_open.capture"


def test_new_file():
    pass
    cf = CaptureFile(file_name_1, to_write=True, force_new_empty_file=True)
    assert cf.record_count() == 0
    cf.close()


def test_str():
    with CaptureFile(file_name_1, to_write=True, force_new_empty_file=True) as cf:
        for i in range(1, 10001):
            cf.add_record(f"Hey this is my record {i:,}")
        LOGGER.info(str(cf))
        cf.close()
        LOGGER.info(str(cf))
        cf.open()
        LOGGER.info(str(cf))


def test_record_out_of_range():
    cf = CaptureFile(file_name_1, to_write=True, force_new_empty_file=True)
    try:
        cf.record_at(0)
    except IndexError:
        LOGGER.info(
            "Got expected index error because you cannot get a record number < 1"
        )
    else:
        cf.close()
        assert False, "Should not be able to get a record number < 1"

    try:
        cf.record_at(1)
    except IndexError:
        LOGGER.info(
            "Got expected index error because you cannot get a record number > record"
            " count"
        )
    else:
        cf.close()
        assert False, "Should not be able to get a record number that hasn't been added"

    cf.close()


def test_setting_and_getting_metadata():
    cf = CaptureFile(file_name_1, to_write=True)
    cf.set_metadata(b"One more metadata update3")
    assert b"One more metadata update3" == cf.get_metadata()
    cf.commit()
    cf.close()


def test_setting_metadata_and_adding_records():
    cf = CaptureFile(
        file_name_1,
        to_write=True,
        initial_metadata=b"This is the initial metadata",
        force_new_empty_file=True,
    )
    assert b"This is the initial metadata" == cf.get_metadata()
    cf.set_metadata(b"Yo, this is my special metadata stuff")
    assert b"Yo, this is my special metadata stuff" == cf.get_metadata()
    for i in range(1, 10001):
        cf.add_record(f"Hey this is my record {i:,}")
        if i % 100 == 0:
            cf.commit()
    assert b"Yo, this is my special metadata stuff" == cf.get_metadata()
    cf.set_metadata(b"No, way yo, this is my real special metadata stuff")
    cf.add_record(f"Hey this is my record {10001:,}")
    assert b"No, way yo, this is my real special metadata stuff" == cf.get_metadata()
    cf.set_metadata(b"No, way yo, this is my real special metadata stuff - part 2")
    assert (
        b"No, way yo, this is my real special metadata stuff - part 2"
        == cf.get_metadata()
    )
    cf.close()


def test_adding_10_000_string_records():
    start = time.time()
    number_of_records = 10_000
    cf = CaptureFile(
        file_name_1,
        to_write=True,
        initial_metadata=b"This is the initial metadata",
        force_new_empty_file=True,
    )
    for i in range(1, number_of_records + 1):
        cf.add_record(f"Hey this is my record {i:,}")
        if i % 1000 == 0:
            cf.commit()
    cf.commit()
    assert cf.record_count() == number_of_records
    end = time.time()
    LOGGER.info(f"There are {cf.record_count():,} records in this file")
    cf.close()
    LOGGER.info(f"Elapsed time writing {number_of_records:,} records = {end-start}s")


def test_10_000_string_record_contents_using_record_at():
    start = time.time()
    number_of_records = 10_000
    cf = CaptureFile(file_name_1)
    for i in range(1, number_of_records + 1):
        record = cf.record_at(i)
        assert record == f"Hey this is my record {i:,}"
    cf.close()
    end = time.time()
    LOGGER.info(
        f"Elapsed time reading and validating {number_of_records:,} records using"
        f" record_at = {end-start}s"
    )


def test_adding_10_000_000_string_records():
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
    LOGGER.info(f"There are {cf.record_count():,} records in this file")
    cf.close()
    LOGGER.info(f"Elapsed time writing {number_of_records:,} records = {end-start}s")


def test_10_000_000_record_contents_using_record_at():
    start = time.time()
    number_of_records = 10_000_000
    cf = CaptureFile(file_name_1)
    for i in range(1, number_of_records + 1):
        record = cf.record_at(i)
        assert record == f"Hey this is my record {i:,}"
    cf.close()
    end = time.time()
    LOGGER.info(
        f"Elapsed time reading and validating {number_of_records:,} records using"
        f" record_at = {end-start}s"
    )


def test_adding_records_after_closing_and_opening_again():
    cf = CaptureFile(file_name_2, to_write=True, force_new_empty_file=True)
    for i in range(1, 101):
        cf.add_record(f"Hey this is my record {i:,}")
    cf.commit()
    cf.close()
    cf = CaptureFile(file_name_2, to_write=True)
    for i in range(101, 201):
        cf.add_record(f"Hey this is my record {i:,}")
    cf.commit()
    assert cf.record_count() == 200
    cf.close()
    cf = CaptureFile(file_name_2)
    for i in range(1, 201):
        record = cf.record_at(i)
        assert record == f"Hey this is my record {i:,}"
    cf.close()


def lock_file_for_a_time():
    print("Starting lock_file_for_a_time")
    LOGGER.info("Starting lock_file_for_a_time")
    cf = CaptureFile(file_name_1, to_write=True, use_os_file_locking=True)
    print("Done")
    LOGGER.info("Done")
    time.sleep(500)
    cf.close()


def test_trying_to_open_for_write_twice_different_process():
    p = Process(target=lock_file_for_a_time)
    p.start()
    time.sleep(1)
    LOGGER.info("File opened for write in another process")
    cfr = CaptureFile(file_name_1, use_os_file_locking=True)
    LOGGER.info("File opened for read")
    try:
        LOGGER.info(
            f"Files opened for write -> \n{CaptureFile._filenames_opened_for_write}"
        )
        cfw2 = CaptureFile(file_name_1, to_write=True, use_os_file_locking=True)
        LOGGER.info("File opened for write - second time")
    except CaptureFileAlreadyOpen:
        LOGGER.info(
            "Got expected error because you cannot open the same capture file twice for"
            " write"
        )
    else:
        cfw2.close()
        assert False, "Should not be able to open the capture file twice"
    p.kill()
    cfr.close()
    LOGGER.info(
        f"Files opened for write -> \n{CaptureFile._filenames_opened_for_write}"
    )


def test_trying_to_open_for_write_twice_same_process():
    LOGGER.info(
        f"Files opened for write -> \n{CaptureFile._filenames_opened_for_write}"
    )
    cfw1 = CaptureFile(file_name_1, to_write=True)
    LOGGER.info("File opened for write in another process")
    cfr = CaptureFile(file_name_1)
    LOGGER.info("File opened for read")
    try:
        cfw2 = CaptureFile(file_name_1, to_write=True)
        LOGGER.info("File opened for write - second time")
    except CaptureFileAlreadyOpen as ex:
        LOGGER.info(
            "Got expected error because you cannot open the same capture file twice for"
            " write"
        )
    else:
        cfw2.close()
        assert False, "Should not be able to open the capture file twice"
    cfw1.close()
    cfr.close()


def lock_master_node_for_a_while(phase: int, use_os_file_locking: bool):
    print(f"Starting lock_master_node_for_a_while - phase: {phase}")
    LOGGER.info(f"Starting lock_master_node_for_a_while - phase: {phase}")
    cf = CaptureFile(file_name_1, use_os_file_locking=use_os_file_locking)
    print(f"Capture file open - phase: {phase}")
    LOGGER.info(f"Capture file open - phase: {phase}")
    cf._acquire_master_nodes_lock_internal(True)
    print(f"Lock acquired - phase: {phase}")
    LOGGER.info(f"Lock acquired - phase: {phase}")
    time.sleep(2)
    cf._acquire_master_nodes_lock_internal(False)
    print(f"Lock released - phase: {phase}")
    LOGGER.info(f"Lock released - phase: {phase}")
    cf.close()


def test_master_node_locking_1(use_os_file_locking: bool = False):
    """Couldn't use paramerization because pytest did them in parallel"""
    path = Path(file_name_1)
    t1 = Thread(target=lock_master_node_for_a_while, args=(1, use_os_file_locking))
    t1.start()
    time.sleep(1)
    print(CaptureFile._filenames_with_master_node_lock[path])
    LOGGER.info(CaptureFile._filenames_with_master_node_lock[path])
    t2 = Thread(target=lock_master_node_for_a_while, args=(2, use_os_file_locking))
    t2.start()
    time.sleep(6)
    assert path not in CaptureFile._filenames_with_master_node_lock


def test_master_node_locking_P1(use_os_file_locking: bool = False):
    """Couldn't use paramerization because pytest did them in parallel"""
    path = Path(file_name_1)
    t1 = Process(target=lock_master_node_for_a_while, args=(1, use_os_file_locking))
    t1.start()
    time.sleep(1)
    t2 = Thread(target=lock_master_node_for_a_while, args=(2, use_os_file_locking))
    t2.start()
    time.sleep(6)
    assert path not in CaptureFile._filenames_with_master_node_lock


def test_master_node_locking_2(use_os_file_locking: bool = True):
    """Couldn't use paramerization because pytest did them in parallel"""
    path = Path(file_name_1)
    t1 = Thread(target=lock_master_node_for_a_while, args=(1, use_os_file_locking))
    t1.start()
    time.sleep(1)
    print(CaptureFile._filenames_with_master_node_lock[path])
    LOGGER.info(CaptureFile._filenames_with_master_node_lock[path])
    t2 = Thread(target=lock_master_node_for_a_while, args=(2, use_os_file_locking))
    t2.start()
    time.sleep(6)
    assert path not in CaptureFile._filenames_with_master_node_lock


def test_master_node_locking_P2(use_os_file_locking: bool = True):
    """Couldn't use paramerization because pytest did them in parallel"""
    path = Path(file_name_1)
    t1 = Process(target=lock_master_node_for_a_while, args=(1, use_os_file_locking))
    t1.start()
    time.sleep(1)
    t2 = Thread(target=lock_master_node_for_a_while, args=(2, use_os_file_locking))
    t2.start()
    time.sleep(6)
    assert path not in CaptureFile._filenames_with_master_node_lock


def test_1_record_at_contents():
    cf = CaptureFile(file_name_1)
    i = 999_638
    record = cf.record_at(i)
    assert record == f"Hey this is my record {i:,}"
    cf.close()


def test_timing_of_iterator():
    start = time.time()
    cfr = CaptureFile(file_name_1, encoding=None)
    number_of_records = 10_000_000
    start_record = 1
    # iter just calls record_generator
    rg = iter(cfr)
    for _ in range(start_record, start_record + number_of_records):
        next(rg)
    cfr.close()
    end = time.time()
    LOGGER.info(
        f"Elapsed time reading {number_of_records:,} records starting at record"
        f" {start_record:,} = {end-start}s"
    )


def test_record_generator_directly():
    start = time.time()
    cfr = CaptureFile(file_name_1)
    number_of_records = 1_000_000
    start_record = 1
    rg = cfr.record_generator(start_record)
    for i in range(start_record, start_record + number_of_records):
        record = next(rg)
        assert record == f"Hey this is my record {i:,}"
    cfr.close()
    end = time.time()
    LOGGER.info(
        f"Elapsed time reading and validating {number_of_records:,} records starting at"
        f" record {start_record:,} = {end-start}s"
    )

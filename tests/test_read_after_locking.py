try:
    import msvcrt  # noqa
    import msvcrt
    import os
    import zlib

    import pytest

    from CaptureFile import CaptureFile

    @pytest.mark.skip(reason="This test is meant to fail to show how Python partial buffer reading is broken for Windows")
    def test_read_after_locking():
    # On 2021-09-26 discovered that on Windows 10 future buffer reads have incorrect
    # bytes after the 4k if read in partial pages. E.g. read 4 bytes then read more
    # than 4k more bytes. The bytes that appeared after the 4k were the original 4
    # bytes read. This only happened if a read from position 0 happened before the
    # lock. Reading a page from the file after the lock seemed to fix the issue

        file_name = R"TempTestFiles\Test1.capture"
        if os.path.exists(file_name):
            os.remove(file_name)
        CaptureFile(file_name, True).close()

        file = open(file_name, mode='rb+')

        file.read(1)

        os.lseek(file.fileno(), 4096, os.SEEK_SET)
        msvcrt.locking(file.fileno(), msvcrt.LK_LOCK, 81920)

        # The following will fix the corrupted buffer

        # file.seek(4096)
        # buffer = file.read(4096)

        file.seek(4096)
        crc_bytes = file.read(4)
        recorded_crc32 = int.from_bytes(crc_bytes, byteorder="big")
        buffer = file.read(40956)
        computed_crc32 = zlib.crc32(buffer) & 0xFFFFFFFF

        os.lseek(file.fileno(), 4096, os.SEEK_SET)
        msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 81920)

        assert(recorded_crc32 == computed_crc32), "Recorded and computed CRC don't match so read failed with the corrupted buffer issue."

        file.close()

except ModuleNotFoundError:
    # This is only a test when on windows
    pass
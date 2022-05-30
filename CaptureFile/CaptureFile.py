"""For an overview of CaptureFile capabilities see README.md in the root of
the CaptureFile repository"""

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import InitVar, dataclass, field
from functools import _lru_cache_wrapper, lru_cache
from io import BytesIO
from itertools import islice
from math import ceil
from os import SEEK_END, SEEK_SET, lseek, remove
from pathlib import Path
from shutil import move
from struct import Struct
from sys import modules
from tempfile import NamedTemporaryFile
from threading import Semaphore
from time import sleep
from typing import IO, ClassVar, Generator, List, Optional, Set, Tuple, Union
from zlib import compress, crc32, decompress

try:
    import msvcrt
except ModuleNotFoundError:
    # Exception will happen when attempting to import msvcrt on an OS other than
    # Windows. When not on Windows the Linux/Unix variant below will be imported
    import fcntl

Record = Union[str, bytes]


@dataclass
class CaptureFile:
    """The CaptureFile constructor opens and returns a capture file named
    `file_name` for reading or writing, depending on the value of `to_write`.

    If the capture file does not already exist and it is opened for write, or if
    `force_new_empty_file` is True, then a new file will be created and the
    initial value for its metadata will be the passed `initial_metadata`.
    These are the only cases where the passed `initial_metadata` is used, and
    it is provided as a way of optionally ensuring that a capture file always
    has metadata even when it is first created.

    The `encoding` argument is used to decode records that are returned. The
    default is `utf8`, which means the binary records stored in the capture file
    will be decoded into strings using the utf8 encoding before being returned.
    If `encoding=None` is set, then the raw bytes will be returned. All of the
    encodings available at
    https://docs.python.org/3/library/codecs.html#standard-encodings are valid.

    Only one process can open a capture file for writing at a time. Multiple
    processes can open the same capture file for read simultaneously with each
    other and with one process that opens it for write.

    An `InvalidCaptureFile` exception is raised if this constructor is used to
    open a file that is not a valid capture file, is in an unsupported version
    of the capture file format, or is a corruptted.
    """

    _compression_level: ClassVar[int] = -1
    """The amount of relative effort, 1 to 9, with which to compress data.

    -1 is the default compromise which currently is equivalent to 6."""

    _lock_start_position: ClassVar[int] = 0x7FFFFFFFFFFFFFFE
    _lock_end_position: ClassVar[int] = 0x7FFFFFFFFFFFFFFF
    _lock_size: ClassVar[int] = _lock_end_position - _lock_start_position

    _filenames_opened_for_write_sem: ClassVar[Semaphore] = Semaphore()
    _filenames_opened_for_write: ClassVar[Set[Path]] = set()
    """For in-process double checking to prevent multiple to-write opens."""

    file_name: str
    to_write: bool = False
    initial_metadata: InitVar[Optional[bytes]] = None
    force_new_empty_file: InitVar[bool] = False
    encoding: Optional[str] = "utf_8"

    _file_name: Path = field(init=False)
    """A "Path" instance of file_name set during __post_init__"""

    _metadata: Optional[bytes] = field(init=False, default=None)

    _config: "CaptureFileConfiguration" = field(init=False)

    _file: Optional[IO[bytes]] = field(init=False, default=None)

    _compression_block: "BytesStream" = field(init=False)

    _current_master_node: "MasterNode" = field(init=False)

    _record_count: int = field(init=False)

    _block_cache: _lru_cache_wrapper = field(init=False)
    _full_node_cache: _lru_cache_wrapper = field(init=False)

    def __post_init__(
        self,
        initial_metadata: Optional[bytes],
        force_new_empty_file: bool,
    ):
        self._block_cache = lru_cache(maxsize=10)(self._block_cache_method)
        self._full_node_cache = lru_cache(maxsize=10)(self._full_node_cache_method)

        self._file_name = Path(self.file_name)

        if force_new_empty_file or (self.to_write and not self._file_name.is_file()):
            self._new_file(initial_metadata)
        self.open(self.to_write)

    def __str__(self):
        if self._file:
            status = f'opened for {"writing" if self.to_write else "reading"}'
        else:
            status = "currently closed but last seen"
        return f'"{self._file_name}" {status} with {self._record_count:,} records'

    def open(self, to_write: bool = False):
        """Opens this CaptureFile instance for reading or writing, depending on
        the value of `to_write`.

        By default, this CaptureFile instance is opened for read unless
        `to_write=True`, in which case it will be opened for write.

        `open` is typically used to reopen a capture file object that was
        previously closed, but it is also called by the constructor.

        Only one instance of CaptureFile, either within or across processes, can
        open a capture file for writing at a time. Multiple instances across one
        more more processes can open the same capture file for read
        simultaneously with each other and with one instance that opens it for
        write.

        `open` cannot be called on an instance of CaptureFile that was already
        opened, although it can be closed and then opened again, potentially
        with a different to_write flag.

        If any of these conditions are violated, then then this method will
        raise a `CaptureFileAlreadyOpen` exception.
        """

        if self._file:
            raise CaptureFileAlreadyOpen(
                f'Capture file "{self.file_name}" is already open.'
            )
        with CaptureFile._filenames_opened_for_write_sem:
            if to_write:
                if self._file_name in CaptureFile._filenames_opened_for_write:
                    # Need to check explicitly because in Linux the same process can
                    # get exclusive locks for the same file repeatedly
                    raise CaptureFileAlreadyOpen(
                        f'Capture file "{self.file_name}" is already open for write.'
                    )
            self.to_write = to_write
            mode = "r+b" if to_write else "rb"
            self._file = open(self.file_name, mode=mode, encoding=None)

            if to_write:
                try:
                    self._acquire_lock_for_writing()
                except (OSError, BlockingIOError) as ex:
                    # Another process has a lock on this file
                    self._file.close()
                    self._file = None
                    raise CaptureFileAlreadyOpen(
                        f'Capture file "{self.file_name}" is already open for write.'
                    )
                CaptureFile._filenames_opened_for_write.add(self._file_name)

        self._config = CaptureFileConfiguration.read(self._file)
        self.refresh()

    def close(self):
        """Closes the OS file and clears the reference to it.

        All uncommitted records and metadata will be lost.

        If this capture file is already closed, then this call does nothing."""

        if self._file is not None:
            self._file.close()
            self._file = None
            if self.to_write:
                with CaptureFile._filenames_opened_for_write_sem:
                    CaptureFile._filenames_opened_for_write.remove(self._file_name)

    def __del__(self):
        self.close()

    def _new_file(self, initial_metadata: Optional[bytes]):
        """Creates a new capture file with name `file_name`.

        If the file already exists, it is overwritten by the newly created file.

        If the optional `initial_metadata` is provided, then it is guaranteed
        to be in the resulting capture file if file creation succeeds."""

        with CaptureFile._filenames_opened_for_write_sem:
            if self._file_name in CaptureFile._filenames_opened_for_write:
                # Need to check explicitly because in Linux the same process can
                # get exclusive locks for the same file repeatedly
                raise CaptureFileAlreadyOpen(
                    f'Capture file "{self.file_name}" is already open for write.'
                )
            CaptureFile._filenames_opened_for_write.add(self._file_name)

        self._config = CaptureFileConfiguration()
        self._init_compression_block()

        # First build the capture file as a temporary file so that we never have
        # a partially constructed (invalid) capture file. The option
        # delete=False is required otherwise it's not possible to rename the
        # temporary file to the desired file_name at the end.
        self._file = NamedTemporaryFile(delete=False)
        self.to_write = True
        temp_file_name = self._file.name
        try:
            self._config.write(self._file)

            self._current_master_node = MasterNode(
                serial_number=0,
                file_limit=self._config.initial_file_limit,
                metadata_pointer=DataCoordinates.null(),
                rightmost_path=RightmostPath(),
                contents_of_last_page=bytearray(self._config.page_size),
                compression_block_contents=self._compression_block.getvalue(),
            )

            self.set_metadata(initial_metadata)
            # Create both current and previous master nodes by committing twice
            self.commit()
            self.commit()

            # The initial size for a new capture file is 100 pages. This is done
            # to minimize fragmentation when records are written to the capture
            # file incrementally.
            self._file.seek(self._config.page_size * 100 - 1)
            self._file.write(b"\0")

            self.close()
            move(temp_file_name, self.file_name)
        finally:
            try:
                remove(temp_file_name)
            except FileNotFoundError:
                pass
        return self

    def _file_limit(self, /):
        return self._current_master_node.file_limit

    def _decode_master_nodes(self, /) -> List[Optional["MasterNode"]]:
        """Return both MasterNodes from the data.

        If a node has a bad CRC, its value will be None"""

        return [
            self._decode_master_node(position)
            for position in self._config.master_node_positions
        ]

    def _decode_master_node(self, node_position: int, /) -> Optional["MasterNode"]:
        """Return a MasterNode from the data starting at node_position.

        If the node has a bad CRC, return None"""
        assert self._file
        self._file.seek(node_position)
        recorded_crc32 = self._file.read(4)
        master_node_buffer = self._file.read(self._config.master_node_size - 4)
        recorded_crc32_int = int.from_bytes(recorded_crc32, byteorder="big")
        computed_crc32 = crc32(master_node_buffer) & 0xFFFFFFFF
        if recorded_crc32_int != computed_crc32:
            return None
        else:
            return MasterNode.new_from(master_node_buffer, self._config.page_size)

    def _fetch_sized_data(self, start_position: int, /) -> bytes:
        buffer = self._fetch_data(start_position, 4)
        size = int.from_bytes(buffer, byteorder="big")
        return self._fetch_data(start_position + 4, size)

    def _fetch_data(self, start_position: int, size: int, /) -> bytes:

        assert self._file
        written_limit = (
            self._file_limit() // self._config.page_size * self._config.page_size
        )
        end_position = start_position + size
        if start_position < written_limit:
            self._file.seek(start_position)
            if end_position <= written_limit:
                # Entirely within the file.
                sized_data = self._file.read(size)
            else:
                # Split between file and unwritten buffer.
                written_size = written_limit - start_position
                unwritten_size = size - written_size
                sized_data = (
                    self._file.read(written_size)
                    + self._current_master_node.contents_of_last_page[0:unwritten_size]
                )
        else:
            # Entirely within the unwritten buffer.
            unwritten_start = start_position - written_limit
            sized_data = self._current_master_node.contents_of_last_page[
                unwritten_start : unwritten_start + size
            ]
        return sized_data

    def _init_compression_block(self, /):
        self._compression_block = BytesStream()

    def refresh(self, /):
        """Updates the internal structures of this capture file object to
        reflect the current state of the file on disk.

        Use `refresh` to allow fetching of records that were added after the
        capture file was opened for read. If the capture file was opened for
        write, then no other process could have added records so refreshing is not
        required.

        If this capture file is not open, then this method will raise a
        `CaptureFileNotOpen` exception."""

        if not self._file:
            raise CaptureFileNotOpen(
                f'Cannot refresh "{self.file_name}" because it is not open.'
            )

        # the main work is done in the private refresh method "_refresh" while
        # this method retries the call in case the master nodes were temporarily
        # not exactly one sequence number apart
        try:
            for retry_count in range(0, 3):
                try:
                    self.__refresh()
                    break
                except InvalidCaptureFile as ex:
                    if retry_count == 0:
                        continue
                    elif retry_count == 1:
                        sleep(4)
                        continue
                    else:
                        raise ex
        except Exception as ex:
            self.close()
            raise ex

    def __refresh(self, /):
        with self._acquire_master_nodes_lock():
            nodes = self._decode_master_nodes()
            if not any(nodes):
                raise InvalidCaptureFile(
                    "Invalid capture file -- both master nodes are corrupt."
                )
            if all(nodes):
                delta = (nodes[0].serial_number - nodes[1].serial_number) & 0xFFFFFFFF
                if delta not in (1, 0xFFFFFFFF):
                    raise InvalidCaptureFile(
                        "Invalid capture file -- master nodes are valid but have"
                        " non-consecutive serial numbers."
                    )
                current_master_node_index = 0 if delta == 1 else 1
            else:
                current_master_node_index = 0 if nodes[0] is not None else 1

            self._current_master_node = nodes[current_master_node_index]

            self._compression_block = BytesStream(
                self._current_master_node.compression_block_contents
            )
            self._compression_block.seek(0, SEEK_END)
            self._record_count = self._current_master_node.compute_record_count(
                self._config.fan_out
            )

    def __enter__(self, /):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _acquire_lock_for_writing(self, /):
        if "msvcrt" in modules:
            # we must be on windows
            lseek(self._file.fileno(), CaptureFile._lock_start_position, SEEK_SET)
            msvcrt.locking(  # noqa
                self._file.fileno(), msvcrt.LK_LOCK, CaptureFile._lock_size  # noqa
            )
        else:
            # we are probably on some Unix variant
            result = fcntl.lockf(
                self._file.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB,
                CaptureFile._lock_size,
                CaptureFile._lock_start_position,
            )

    @contextmanager
    def _acquire_master_nodes_lock(self, /):
        self._acquire_master_nodes_lock_internal(True)
        try:
            yield
        finally:
            self._acquire_master_nodes_lock_internal(False)

    def _acquire_master_nodes_lock_internal(self, lock: bool, /):
        assert self._file
        lock_size = self._config.master_node_size * 2
        if "msvcrt" in modules:
            lseek(self._file.fileno(), self._config.page_size, SEEK_SET)
            # we must be on windows
            # added comments below to suppress my-py errors when we are viewing code on Linux
            lock_mode = msvcrt.LK_LOCK if lock else msvcrt.LK_UNLCK  # type: ignore[attr-defined]
            msvcrt.locking(self._file.fileno(), lock_mode, lock_size)  # type: ignore[attr-defined]

            # On 2021-09-26 discovered that on Windows 10 buffer reads after the
            # lock have incorrect bytes after the first 4k bytes if read in
            # partial pages. E.g. read 4 bytes then read more than 4k more
            # bytes. The bytes that appeared after the 4k were the original 4
            # bytes read. This only happened if a read from position 0 happened
            # before the lock. Reading a page from the file after the lock
            # seemed to fix the issue. Reading more than 4k to start did not
            # help
            self._file.seek(self._config.page_size)
            self._file.read(self._config.page_size)
        else:
            # we are probably on some Unix variant
            # added comments below to suppress my-py errors when we are viewing code on Windows
            lock_type = fcntl.LOCK_EX if self.to_write else fcntl.LOCK_SH  # type: ignore[attr-defined]
            lock_mode = lock_type if lock else fcntl.LOCK_UN  # type: ignore[attr-defined]
            fcntl.lockf(  # type: ignore[attr-defined]
                self._file.fileno(),
                lock_mode,
                lock_size,
                self._config.page_size,
            )

    def get_metadata(self, /) -> Optional[bytes]:
        """Returns the binary metadata that was stored in the capture file on
        creation or using `set_metadata`.

        If there is no metadata set, then None is returned.

        If this capture file is not open, then this method will raise a
        `CaptureFileNotOpen` exception."""

        if not self._file:
            raise CaptureFileNotOpen(
                f'Cannot refresh "{self.file_name}" because it is not open.'
            )

        if self._current_master_node.metadata_pointer.is_null():
            return None
        else:
            if self._metadata is None:
                self._metadata = (
                    self._current_master_node.metadata_pointer.sized_data_block(self)
                ).tobytes()
            return self._metadata

    def set_metadata(self, new_metadata: Optional[bytes], /):
        """Stores binary data in this capture file that can be retrieved with
        `get_metadata`. Metadata does not appear as a record, but is instead
        associated with the capture file as a whole.

        Since `new_metadata` is saved transactionally with records, it can be
        used to remember where processing left off at the last successful
        commit. This makes the knowledge of exactly where to restart processing
        available for recovery after a failure.

        The binary contents of `new_metadata` are completely up to the user; the
        capture file only sets and gets this data.

        To clear the metadata, `None` can be passed in for `new_metadata`.

        If this capture file is not open, then this method will raise a
        `CaptureFileNotOpen` exception.

        If it is not open for write then it will raise a
        `CaptureFileNotOpenForWrite` exception."""

        if not self._file:
            raise CaptureFileNotOpen(
                f'Cannot set the metadata of "{self.file_name}" because it is not open.'
            )

        if not self.to_write:
            raise CaptureFileNotOpenForWrite(
                f'Cannot set the metadata of "{self.file_name}" because it is not open'
                " for writting."
            )

        self._metadata = new_metadata
        self._current_master_node.metadata_pointer = (
            DataCoordinates.null()
            if new_metadata is None
            else self._add_data_block(new_metadata)
        )

    def __iter__(self, /):
        return self.record_generator()

    def __getitem__(self, key: Union[int, slice], /) -> Union[List[Record], Record]:
        if isinstance(key, slice):
            if key.step in (1, None):
                # If we are stepping by +1 then the most efficient method is to
                # use a record_generator starting at key.start, for anything
                # else we may as well use the record_at method and fetch each
                # record directly.
                start = 1 if key.start is None else key.start
                if key.stop is None:
                    return list(self.record_generator(key.start))
                else:
                    return list(
                        islice(self.record_generator(start), key.stop - key.start)
                    )
            else:
                indices = range(*key.indices(self._record_count))
                return [self.record_at(i) for i in indices]
        return self.record_at(key)

    def record_generator(
        self, starting_record_number: int = 1
    ) -> Generator[Record, None, None]:
        """Returns a generator of records begining at `starting_record_number`
        that continues until the end of the file as it existed when
        `record_generator` was called.

        This is used internally by `__iter__` to provide all the standard
        iteration capabilities on capture file starting at record 1.

        This is also used internally by `__getitem__` to efficiently support
        contiguous ranges; e.g. to return the records from 999 to 1010 on a
        capture file cf use: cf[999:1011]. Non-contiguous ranges are supported
        by `record_at`.

        If this capture file is not open, then this method will raise a
        `CaptureFileNotOpen` exception."""

        if not self._file:
            raise CaptureFileNotOpen(
                f'Cannot set iterate over the records of "{self.file_name}" because it'
                " is not open."
            )

        if starting_record_number < 1:
            raise IndexError

        # only the rightmost path is mutable so grabing a copy of it ensures
        # that the generator can continue to work even if records are inserted
        # after the generator was created although the new records will not be
        # returned. If that is desired then a new record_generator should be
        # requested if the number of records has increased post creation of the
        # record_generator
        rightmost_path = deepcopy(self._current_master_node.rightmost_path)

        height = rightmost_path.number_of_levels()
        return self._record_generator(
            starting_record_number - 1,
            rightmost_path,
            height,
            self._config.fan_out ** height,
        )

    def _record_generator(
        self,
        index_remaining: int,
        rightmost_path: "RightmostPath",
        height: int,
        power: int,
        /,
    ) -> Generator[Record, None, None]:

        rightmost_node = rightmost_path.rightmost_node(height)
        power = power // self._config.fan_out

        (starting_child_index, index_remaining) = divmod(index_remaining, power)

        for child_index in range(starting_child_index, rightmost_node.child_count()):
            child_node = rightmost_node.children[child_index]
            if height == 1:
                yield child_node.record(self)
            else:
                yield from self._record_generator_for_perfect_subtree(
                    index_remaining,
                    child_node,
                    height - 1,
                    power,
                )
                index_remaining = 0
        if height > 1:
            yield from self._record_generator(
                index_remaining, rightmost_path, height - 1, power
            )

    def _record_generator_for_perfect_subtree(
        self,
        index_remaining: int,
        starting_node: "DataCoordinates",
        height: int,
        power: int,
        /,
    ) -> Generator[Record, None, None]:
        power = power // self._config.fan_out

        (starting_child_index, index_remaining) = divmod(index_remaining, power)

        block = self._block(starting_node.compressed_block_start)
        offset = (
            starting_node.data_start
            + DataCoordinates.struct.size * starting_child_index
        )

        for _ in range(starting_child_index, self._config.fan_out):
            child_node = DataCoordinates.from_bytes(block, offset)
            offset += DataCoordinates.struct.size
            if height == 1:
                yield child_node.record(self)
            else:
                yield from self._record_generator_for_perfect_subtree(
                    index_remaining, child_node, height - 1, power
                )
                index_remaining = 0

    def record_at(self, record_number: int, /) -> Record:
        """Returns the record stored at the passed `record_number`.

        The first record in the file is at `record_number` = 1.

        If the CaptureFile was opened with `encoding=None`, then the returned
        value will be the raw bytes of the record. Otherwise, the returned value
        will be a string created from the binary data using the encoding
        specified when the constructor was called.

        An attempt to get a record before the first record or beyond the last
        record available will raise an `IndexError` exception.

        If this capture file is not open, then this method will raise a
        `CaptureFileNotOpen` exception."""

        if not self._file:
            raise CaptureFileNotOpen(
                f'Cannot get record from "{self.file_name}" because it is not open.'
            )

        if record_number < 1 or record_number > self._record_count:
            raise IndexError
        rightmost_nodes = self._current_master_node.rightmost_path.rightmost_nodes
        # Use "reversed" so we start at the root instead of the leaves
        root_to_leaf_rightmost_nodes = reversed(rightmost_nodes)
        root_to_leaf_path = reversed(
            leaf_to_root_path(
                # 1 is subtracted from record_number because Python is
                # 0-based while CaptureFile records start at 1
                record_number - 1,
                len(rightmost_nodes),
                self._config.fan_out,
            )
        )

        # skip nodes as long as path follows rightmost nodes.
        for child_index, current_rightmost_node in zip(
            root_to_leaf_path, root_to_leaf_rightmost_nodes
        ):
            if child_index != len(current_rightmost_node.children):
                break

        # get first persistent child's data cooridnates. This child will refer
        # to either the record or the root of a perfect sub-tree of which no
        # decendant can be a rightmost node of the top level tree.
        current_child_coordinates = current_rightmost_node.children[child_index]

        # iterate through the remainder of the path of child indexes until we
        # arrive at the data coordinates of the record
        for child_index in root_to_leaf_path:
            current_child_coordinates = self._full_node_cache(
                current_child_coordinates
            )[child_index]

        return current_child_coordinates.record(self)

    def record_count(self, /) -> int:
        """Returns the number of records available when the file was opened or
        last refreshed. If opened for write, the record count is up-to-date with
        any records that have been added even if they have not been
        committed."""
        return self._record_count

    def _full_node_cache_method(
        self, child: "DataCoordinates", /
    ) -> Tuple["DataCoordinates", ...]:
        block = self._block(child.compressed_block_start)
        tup = self._config.full_node_struct.unpack_from(block, child.data_start)
        return tuple(DataCoordinates(tup[i], tup[i + 1]) for i in range(0, len(tup), 2))

    def _block(self, file_position: int, /) -> memoryview:
        # The block cache never needs to be cleared, because it only holds full
        # blocks and since capture files are append only, a full block can never
        # change.
        return (
            # The final block which can be incomplete is stored in the master
            # node in the compression_block_contents, and is always available so
            # it is not cached but just returned if file_position is at
            # file_limit.
            self._compression_block.getbuffer()
            if file_position == self._file_limit()
            else self._block_cache(file_position)
        )

    def _block_cache_method(self, file_position: int, /) -> memoryview:
        compressed_bytes = self._fetch_sized_data(file_position)
        uncompressed_bytes = decompress(compressed_bytes)
        return memoryview(uncompressed_bytes)

    def _file_size(self, /) -> int:
        assert self._file
        self._file.seek(0, SEEK_END)
        return self._file.tell()

    def _write_full_pages(self, raw_bytes: bytes, /):
        """Append `raw_bytes` to the end of the file data (file_limit) in full
        page increments.

        Remaining partial page data is held in the master node until next time
        when it is written as the begining of the first full page."""

        assert self._file
        pos_in_last_page = self._file_limit() % self._config.page_size
        total_len = pos_in_last_page + len(raw_bytes)
        full_pages_len = total_len // self._config.page_size * self._config.page_size
        if full_pages_len > 0:
            self._file.seek(
                self._file_limit() // self._config.page_size * self._config.page_size
            )
            self._file.write(
                self._current_master_node.contents_of_last_page[:pos_in_last_page]
            )
            full_page_remainder_len = full_pages_len - pos_in_last_page
            self._file.write(raw_bytes[:full_page_remainder_len])
            raw_bytes_remainder_len = len(raw_bytes) - full_page_remainder_len
            unwritten_page_len = self._config.page_size - raw_bytes_remainder_len
            self._current_master_node.contents_of_last_page[
                :raw_bytes_remainder_len
            ] = raw_bytes[full_page_remainder_len:]
            self._current_master_node.contents_of_last_page[
                raw_bytes_remainder_len:
            ] = (b"\x00" * unwritten_page_len)
        else:
            self._current_master_node.contents_of_last_page[
                pos_in_last_page:total_len
            ] = raw_bytes
        self._current_master_node.file_limit += len(raw_bytes)

    def _compress_and_write_if_full(self, /):
        if self._compression_block.tell() >= self._config.compression_block_size:
            # The compression block is full. Compress it and write it to the file.
            compressed = compress(
                self._compression_block.getvalue(), CaptureFile._compression_level
            )
            self._init_compression_block()
            file_size = self._file_size()
            if self._file_limit() + 4 + len(compressed) > file_size:
                # Grow file by 5MB at a time (but never more than doubling) to
                # avoid fragmentation. Prevent the file.truncate() below from
                # attempting to re-read whatever page of data is at the current
                # position.  Otherwise it could in theory conflict with a lock
                # held by another OS process, leading to failure. Positioning it
                # to zero is safe, since that page contains the file's permanent
                # metadata and is never rewritten.
                self._file.seek(0, SEEK_SET)
                growth = (
                    ceil(min(5242880, self._file_limit()) / self._config.page_size)
                    * self._config.page_size
                )
                self._file.truncate(file_size + growth)
            int_as_bytes = int.to_bytes(len(compressed), 4, "big", signed=False)
            self._write_full_pages(int_as_bytes)
            self._write_full_pages(compressed)

    def _coordinates_for_next_new_data_block(self, /) -> "DataCoordinates":
        return DataCoordinates(self._file_limit(), self._compression_block.tell())

    def _add_data_block(self, data_block: bytes, /) -> "DataCoordinates":
        """Add the passed data block to the file without committing it and return its
        cooridnates"""

        coordinates_for_new_record = self._coordinates_for_next_new_data_block()
        self._compression_block.write_sized(data_block)
        self._compress_and_write_if_full()
        return coordinates_for_new_record

    def add_record(self, record: Record, /) -> int:
        """Adds the passed `record` to this capture file without committing it
        and returns the new record count.

        All records added to a capture file are binary. If a string is passed to
        `add_record`, it will automatically be encoded to binary before being
        stored.

        If this capture file is not open, then this method will raise a
        `CaptureFileNotOpen` exception.

        If the capture file is open for read but not for write, then it will
        raise a `CaptureFileNotOpenForWrite` exception."""

        if not self._file:
            raise CaptureFileNotOpen(
                f'Cannot add a record to "{self.file_name}" because it is not open.'
            )

        if not self.to_write:
            raise CaptureFileNotOpenForWrite(
                f'Cannot add a record to "{self.file_name}" because it is not open for'
                " writting."
            )

        self._current_master_node.rightmost_path.add_child_to_rightmost_node(
            self._add_data_block(
                record if isinstance(record, bytes) else record.encode()
            ),
            1,
            self,
        )
        self._record_count += 1
        return self._record_count

    def _write_master_node(self, /):
        self._current_master_node.compression_block_contents = (
            self._compression_block.getvalue()
        )
        master_node_buffer = self._current_master_node.as_bytes(self._config)
        crc = crc32(master_node_buffer) & 0xFFFFFFFF
        crc_as_bytes = int.to_bytes(crc, 4, "big", signed=False)
        self._file.write(crc_as_bytes)
        self._file.write(master_node_buffer)

    def commit(self, /):
        """Commits records added to the capture file and any metadata
        that was set since the last commit or, if there was no previous commit,
        since this capture file was opened for write.

        No records added or metadata that was set will be persistent or visible
        to other processes until committed by this method.

        Either all records and metadata will be committed, or, in the case of
        failure, no records or metadata will be committed.

        If this capture file is not open, then this method will raise a
        `CaptureFileNotOpen` exception.

        If it is not open for write then this method will raise a
        `CaptureFileNotOpenForWrite` exception."""

        if not self._file:
            raise CaptureFileNotOpen(
                f'Cannot commit "{self.file_name}" because it is not open.'
            )

        if not self.to_write:
            raise CaptureFileNotOpenForWrite(
                f'Cannot commit "{self.file_name}" because it is not open for writting.'
            )

        self._file.flush()
        self._current_master_node.increment_serial_number()
        with self._acquire_master_nodes_lock():
            self._file.seek(self._current_master_node.position(self._config))
            self._write_master_node()
            self._file.flush()


@dataclass
class CaptureFileConfiguration:
    """The persistent configuration values of the capture file that are stored
    in the first bytes of the file.

    Includes the configuration values that can be completely computed from only
    the stored values.

    Default values are provided if a new instance of this class is created
    directly from its constructor"""

    version: int = 2
    """The version indicates the compatability of code with file structure.

    Code with a version higher than the one stored in file should be capable of
    reading and writing to the file but a file with a higher version number than
    what is in the code will not be usable."""

    page_size: int = 4096
    """Pages of page_size bytes are used in various places as a minimum block of
    data. See DESIGN.md for how pages are used."""

    compression_block_size: int = 32768
    """Minimum number of bytes to compress and write out. While data is
    accumulating it is recorded in the master node but after this limit is
    exceeded it will be compressed and written out"""

    fan_out: int = 32
    """The maximum number of children in the index tree's nodes. For more
    information about the tree structure and usage see DESIGN.md"""

    master_node_size: int = field(init=False)

    master_node_positions: Tuple[int] = field(init=False)
    """The two starting byte positions in the file of the two master nodes"""

    compression_block_start: int = field(init=False)
    initial_file_limit: int = field(init=False)
    full_node_struct: Struct = field(init=False)

    current_version: ClassVar[int] = 2
    """The code's current verision which can support any earlier version
    recorded in the file"""

    capture_file_type: ClassVar[bytes] = b"MioCapture\0"

    struct: ClassVar[Struct] = Struct(f">{len(capture_file_type)}s4L")
    """Struct = String("MioCapture\0"), Long(version), Long(page_size),
    Long(compression_block_size), Long(fan_out)"""

    def __post_init__(self, /):
        assert (
            self.compression_block_size % self.page_size == 0
        ), "compression block size must be a multiple of page size"

        self.master_node_size = self.page_size * 2 + self.compression_block_size

        # The first master_node starts at page_size because the entire first
        # page is reserved for the permantent file metadata even though very
        # little of it is used. This fact is also used in
        # compress_and_flush_if_full to know for certain no writing is happening
        # on the first page after the file is created even across multiple OS
        # process.
        self.master_node_positions = [
            self.page_size,
            self.page_size + self.master_node_size,
        ]
        last_master_page_start = self.page_size - 4
        last_master_page_end = last_master_page_start + self.page_size
        self.compression_block_start = last_master_page_end
        self.initial_file_limit = self.master_node_positions[1] + self.master_node_size
        self.full_node_struct = Struct(">" + "QL" * self.fan_out)

    @classmethod
    def read(cls, file, /) -> "CaptureFileConfiguration":
        file.seek(0)
        buffer = file.read(cls.struct.size)
        (
            header,
            version,
            page_size,
            compression_block_size,
            fan_out,
        ) = cls.struct.unpack(buffer)

        if header != cls.capture_file_type and header != b"WebCapture\0":
            # b"WebCapture\0" is old name once used in the header
            raise InvalidCaptureFile(f"{file.name} is not a valid capture file")

        if version > cls.current_version:
            raise InvalidCaptureFile(
                f"{file.name} was created in version {version} format. The highest"
                f" version supported by this program is {cls.current_version}."
            )
        return cls(version, page_size, compression_block_size, fan_out)

    def write(self, file, /):
        buffer = bytearray(self.initial_file_limit)
        CaptureFileConfiguration.struct.pack_into(
            buffer,
            0,
            CaptureFileConfiguration.capture_file_type,
            self.current_version,
            self.page_size,
            self.compression_block_size,
            self.fan_out,
        )
        file.write(buffer)


@dataclass
class MasterNode:
    """
    A MasterNode tracks where things are in the capture file.

    There are two MasterNodes recorded in the capture file so that there is
    always a backup of the previous MasterNode if there is a problem while
    writting out the current MasterNode (for example if the computer is turned
    off half way through the write)
    """

    struct: ClassVar[Struct] = Struct(f">LQL")
    """Struct = serial_number, file_limit, compression_block_len ">LQL" """

    serial_number: int
    """MasterNode with largest serial_number is the active one

    serial_number wraps at size of long such that 0 > FFFFFFFF

    The two MasterNodes' serial_numbers should always be 1 apart"""

    file_limit: int
    """The next location to start writing

    This is not the end of the file since the file is only grown in set
    increments"""

    metadata_pointer: "DataCoordinates"
    """Coordinates of metadata that is recorded in the capture file but not
    otherwise used by the capture file."""

    rightmost_path: "RightmostPath"

    contents_of_last_page: bytearray
    """Tracks the last partial page of data since only full pages are written to
    the end of the capture file

    end = file_limit, not the actual end of the file"""

    compression_block_contents: bytes
    """Place to store data that will eventually be compressed and written out at
    the file_limit once there is  at least compression_block_size data
    present"""

    @classmethod
    def new_from(cls, master_node_buffer: bytes, page_size: int, /) -> "MasterNode":
        (serial_number, file_limit, compression_block_len) = cls.struct.unpack_from(
            master_node_buffer, 0
        )

        compression_block_start = page_size * 2 - 4
        compression_block_end = compression_block_start + compression_block_len
        metadata_pointer = DataCoordinates.from_bytes(
            master_node_buffer, cls.struct.size
        )
        rightmost_path = RightmostPath(
            master_node_buffer, cls.struct.size + DataCoordinates.struct.size
        )
        return cls(
            serial_number=serial_number,
            file_limit=file_limit,
            metadata_pointer=metadata_pointer,
            rightmost_path=rightmost_path,
            # since the 4-byte crc is not in the master_node_buffer but the
            # "page size" did include it, subtract 4 to correct for this
            contents_of_last_page=bytearray(
                master_node_buffer[page_size - 4 : compression_block_start]
            ),
            compression_block_contents=master_node_buffer[
                compression_block_start:compression_block_end
            ],
        )

    def compute_record_count(self, fan_out: int, /) -> int:
        return self.rightmost_path.compute_record_count(fan_out)

    def increment_serial_number(self, /):
        self.serial_number += 1
        self.serial_number &= 0xFFFFFFFF  # Truncate to a "long". It is okay to wrap

    def position(self, config: CaptureFileConfiguration, /) -> int:
        """Return the absolute starting position of where to start writing the
        current master node in the capture file.

        There are two possible starting postitions for the two possible
        alternate master nodes.

        A master node with an odd serial number is written at the first position
        while a master node with an even serial number is written at the second
        position"""
        return config.master_node_positions[self.serial_number % 2]

    def as_bytes(self, config: CaptureFileConfiguration, /) -> bytes:
        """Returns a binary representation of this MasterNode for writing"""

        stream = BytesStream()

        stream.write(
            MasterNode.struct.pack(
                self.serial_number,
                self.file_limit,
                len(self.compression_block_contents),
            )
        )

        self.metadata_pointer.write_data_coordinate(stream)
        self.rightmost_path.write_rightmost_nodes(stream)
        assert (
            stream.tell() <= config.page_size - 4
        ), "Too many RightmostNodes to fit on a page."

        # Align to page
        stream.zero_fill_to(config.page_size - 4)

        stream.write(self.contents_of_last_page)
        stream.write(self.compression_block_contents)

        stream.zero_fill_to(config.master_node_size - 4)
        return stream.getvalue()


@dataclass
class RightmostPath:
    """A list of RightmostNodes in height order (leaf -> root), one for each
    level in the tree.

    These RightmostNodes are where all updates happen when adding data to the
    file"""

    number_of_children_struct: ClassVar[Struct] = Struct(">L")
    """Big-endian unsigned long ">L" """

    rightmost_nodes: List["RightmostNode"] = field(default_factory=list, init=False)

    buffer: InitVar[Optional[bytes]] = None
    offset: InitVar[int] = 0

    def __post_init__(self, buffer: Optional[bytes], offset: int, /):
        if buffer is not None:
            (
                total_number_of_children,
            ) = RightmostPath.number_of_children_struct.unpack_from(buffer, offset)
            offset += RightmostPath.number_of_children_struct.size
            for _ in range(total_number_of_children):
                # each child is preceded by its RightmostNode's height in the
                # tree so the same height will be repeated for each of that
                # RightmostNode's children
                (
                    height,
                    data_coordinate,
                ) = DataCoordinates.from_bytes_with_height_prefix(buffer, offset)
                offset += DataCoordinates.height_prefix_struct.size
                self.rightmost_node(height).add_child(data_coordinate)

    def rightmost_node(self, height: int, /) -> "RightmostNode":
        """Return the RightmostNode for the passed height.

        If the passed height is greater than the current number of levels in
        this RightmostPath, then first create a RightmostNode for each missing
        level"""

        if height > len(self.rightmost_nodes):
            # Some levels might be missing when reading RightmostNodes back in
            # from from data, because an empty RightmostNode would not have
            # outputted any children so its height would not be in the data.
            #
            # This is why an empty RightmostNode needs to be created for each
            # height that is not present in the data.
            new_levels_needed = height - len(self.rightmost_nodes)
            self.rightmost_nodes.extend(
                RightmostNode() for _ in range(new_levels_needed)
            )
        return self.rightmost_nodes[height - 1]

    def compute_record_count(self, fan_out: int, /) -> int:
        power = 1
        record_count = 0
        for rightmost_node in self.rightmost_nodes:
            record_count += rightmost_node.child_count() * power
            power = power * fan_out
        return record_count

    def number_of_levels(self, /) -> int:
        return len(self.rightmost_nodes)

    def decendant_count(self, /) -> int:
        """Total number of children referenced across all RightmostNodes"""

        return sum(
            rightmost_node.child_count() for rightmost_node in self.rightmost_nodes
        )

    def add_child_to_rightmost_node(
        self,
        child_coordinates: "DataCoordinates",
        rightmost_node_height: int,
        capture_file: CaptureFile,
        /,
    ):
        destination_rightmost_node = self.rightmost_node(rightmost_node_height)
        destination_rightmost_node.add_child(child_coordinates)

        if destination_rightmost_node.is_full(capture_file._config.fan_out):
            coordinates_where_full_node_is_written = (
                capture_file._coordinates_for_next_new_data_block()
            )
            # writing will start at: coordinates_where_full_node_is_written
            destination_rightmost_node.write_without_height(
                capture_file._compression_block
            )
            destination_rightmost_node.reset()

            capture_file._compress_and_write_if_full()

            # Since the result of adding a child to the provided RightmostNode
            # caused the RightmostNode to become full and written, we now need
            # to add the full node that was written as a child to it's parent
            self.add_child_to_rightmost_node(
                coordinates_where_full_node_is_written,
                rightmost_node_height + 1,
                capture_file,
            )

    def write_rightmost_nodes(self, stream: "BytesStream", /):
        stream.write_long(self.decendant_count())
        for height, rightmost_node in enumerate(self.rightmost_nodes, start=1):
            # height number starts at 1 not 0 like python lists
            rightmost_node.write_with_height(stream, height)


@dataclass
class RightmostNode:
    """This is the rightmost node of a level in the tree index of all records
    and is not referred to by any parent node.

    RightmostNodes are stored in the MasterNode and are never found full.

    Once a RightmostNode becomes full, it is added as the next child of the
    higher level's RightmostNode.

    If the RightmostNode was already the root, then a new root level
    RightmostNode is created first.

    The root is always a RightmostNode.

    Full RightmostNodes are written as a "full node" at the current file_limit
    and the "full node" never modified again.

    After a full RightmostNode is written, it is cleared of its children (reset)
    and ready to be filled again.
    """

    children: List["DataCoordinates"] = field(default_factory=list, init=False)

    def add_child(self, data_coordinate: "DataCoordinates", /):
        self.children.append(data_coordinate)

    def is_full(self, fan_out, /) -> bool:
        return len(self.children) == fan_out

    def write_with_height(self, stream: "BytesStream", height: int, /):
        """Output to stream in the form for storage in the MasterNode section of the
        capture file"""

        for data_coordinate in self.children:
            stream.write_byte(height)
            data_coordinate.write_data_coordinate(stream)

    def write_without_height(self, stream: "BytesStream", /):
        """Output to stream in the form for storage in the data section of the
        capture file"""

        for data_coordinate in self.children:
            data_coordinate.write_data_coordinate(stream)

    def reset(self, /):
        """Clear out the children making this an empty RightmostNode"""

        self.children.clear()

    def child_count(self, /) -> int:
        return len(self.children)


@dataclass(frozen=True)
class DataCoordinates:
    """The two-dimensional coordinates of data within a capture file.

    The first axis is the absolute position within the capture file of the
    compressed block containing the data.

    The second axis is the position of the data within the uncompressed
    block."""

    struct: ClassVar[Struct] = Struct(">QL")
    """Big-endian unsigned long-long, unsigned long ">QL" """

    height_prefix_struct: ClassVar[Struct] = Struct(">BQL")
    """Big-endian unsigned char, unsigned long-long, unsigned long ">BQL" """

    block_size_struct: ClassVar[Struct] = Struct(">L")
    """Big-endian unsigned long ">L" """

    compressed_block_start: int
    """The start position of the compressed block in capture file"""

    data_start: int
    """The start position of the desired data within the uncompressed data of
    the compressed block"""

    @classmethod
    def from_bytes(cls, block: bytes, offset: int, /) -> "DataCoordinates":
        return cls(*cls.struct.unpack_from(block, offset))

    @classmethod
    def from_bytes_with_height_prefix(
        cls, block: bytes, offset: int, /
    ) -> Tuple[int, "DataCoordinates"]:
        (
            height,
            compressed_block_start,
            data_start,
        ) = cls.height_prefix_struct.unpack_from(block, offset)
        return (
            height,
            cls(
                compressed_block_start,
                data_start,
            ),
        )

    @classmethod
    def null(cls, /) -> "DataCoordinates":
        return cls(0, 0)

    def write_data_coordinate(self, stream: "BytesStream", /):
        stream.write(
            DataCoordinates.struct.pack(self.compressed_block_start, self.data_start)
        )

    def is_null(self, /) -> bool:
        return self.compressed_block_start == 0 and self.data_start == 0

    def sized_data_block(self, capture_file: CaptureFile, /) -> memoryview:
        """Return the block of data located at this DataCoordinates"""

        block = capture_file._block(self.compressed_block_start)
        (data_size,) = DataCoordinates.block_size_struct.unpack_from(
            block, self.data_start
        )
        start = self.data_start + DataCoordinates.block_size_struct.size
        return block[start : start + data_size]

    def record(self, capture_file: CaptureFile, /) -> Record:
        record = self.sized_data_block(capture_file)
        return (
            str(record, capture_file.encoding)
            if capture_file.encoding
            else bytes(record)
        )


class BytesStream(BytesIO):
    def next_long(self, /) -> int:
        return int.from_bytes(self.read(4), "big", signed=False)

    def next_long_long(self, /) -> int:
        return int.from_bytes(self.read(8), "big", signed=False)

    def next(self, /) -> int:
        return int.from_bytes(self.read(1), "big", signed=False)

    def next_sized(self, /) -> bytes:
        size = self.next_long()
        return self.read(size)

    def write_sized(self, data: bytes, /):
        size = len(data)
        size_bytes = int.to_bytes(size, 4, "big", signed=False)
        self.write(size_bytes)
        self.write(data)

    def write_byte(self, integer: int, /):
        int_as_bytes = int.to_bytes(integer, 1, "big", signed=False)
        self.write(int_as_bytes)

    def write_long(self, integer: int, /):
        int_as_bytes = int.to_bytes(integer, 4, "big", signed=False)
        self.write(int_as_bytes)

    def write_long_long(self, integer: int, /):
        int_as_bytes = int.to_bytes(integer, 8, "big", signed=False)
        self.write(int_as_bytes)

    def zero_fill_to(self, end_position: int, /):
        self.write(b"\0" * (end_position - self.tell()))


class CaptureFileAlreadyOpen(Exception):
    pass


class CaptureFileNotOpen(Exception):
    pass


class CaptureFileNotOpenForWrite(Exception):
    pass


class InvalidCaptureFile(Exception):
    pass


def leaf_to_root_path(position: int, height: int, fan_out: int, /) -> List[int]:
    """Compute the path of child indexes from the leaf through the nodes to the
    root."""

    path = [0] * height
    for i in range(height):
        position, path[i] = divmod(position, fan_out)

    return path

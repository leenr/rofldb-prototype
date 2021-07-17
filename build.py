import heapq
import math
from contextlib import contextmanager
from io import BytesIO
from struct import pack
from typing import Iterable, Tuple


def pwrite(f, offset: int, data: bytes):
    old_tell = f.tell()
    f.seek(offset)
    f.write(data)
    f.seek(old_tell)


@contextmanager
def write_size(b: BytesIO, *, pack_format: str):
    size_offset = b.tell()
    b.write(pack(pack_format, 0))  # will be filled later
    start_offset = b.tell()
    yield
    pwrite(b, size_offset, pack(pack_format, b.tell() - start_offset))


def get_btree_level(idx: int):
    if idx == 0:
        return 0
    level = 1
    idx_shifted = idx
    while idx_shifted and idx_shifted % 2 == 0:
        idx_shifted >>= 1
        level += 1
    return level


def pack_tree(key_offsets: Iterable[Tuple[str, int]], write_to: BytesIO):
    write_node_offsets_to = {}
    node_offsets = []

    tree_base = write_to.tell()
    write_to.write(pack('<L', 2 ** 32 - 1))  # will be filled later

    prev_key = None
    for idx, (key, value_offset) in enumerate(key_offsets):
        if prev_key is not None:
            assert key > prev_key, f'Unsorted: {key} !> {prev_key}'
        prev_key = key

        offset = write_to.tell() - tree_base
        if (write_node_offset_to := write_node_offsets_to.pop(idx, None)) is not None:
            # print('write:', idx, write_node_offset_to)
            pwrite(write_to, write_node_offset_to, pack('<L', offset))
        node_offsets.append(offset)

        with write_size(write_to, pack_format='<H'):
            write_to.write(pack('<H', len(key)))
            write_to.write(key.encode('utf-8'))

            write_to.write(pack('<Q', value_offset))

            level = get_btree_level(idx)
            if level > 1 or idx == 1:
                delta = max(1, 2 ** (level - 2))
                write_to.write(pack('<L', node_offsets[idx - delta]))
                if level > 1:
                    while idx + delta >= len(key_offsets):
                        delta //= 2
                    if delta:
                        assert (idx + delta) not in write_node_offsets_to
                        write_node_offsets_to[idx + delta] = write_to.tell()
                        # print('add:', idx, idx + delta, write_to.tell())
                        write_to.write(pack('<L', 2 ** 32 - 1))
                        print(idx, f'({offset}, {key})', idx - delta, idx + delta)
                    else:
                        print(idx, f'({offset}, {key})', idx - delta)
                else:
                    print(idx, f'({offset}, {key})', idx - delta)
            else:
                print(idx, f'({offset})')

    root_idx = 2 ** (math.ceil(math.log(idx + 1, 2)) - 1)
    pwrite(write_to, tree_base, pack('<L', node_offsets[root_idx]))


def pack_into_file(filename: str, key_n_values: Iterable[Tuple[str, str]]):
    with open(filename, 'wb') as f:
        f.write(b'ROFL\x00\x00')

        sorted_key_n_offsets = []

        with write_size(f, pack_format='<Q'):
            value_base = f.tell()
            for key, value in key_n_values:
                value = value.encode('utf-8')
                value_offset = f.tell() - value_base
                f.write(pack('<L', len(value)))
                f.write(value)
                sorted_key_n_offsets.append((key, value_offset))

        sorted_key_n_offsets.sort(key=lambda kv: kv[0])
        with write_size(f, pack_format='<L'):
            pack_tree(sorted_key_n_offsets, f)


pack_into_file(
    'test_pack_python.rofldb',
    ((f'key{i}', f'value{i}') for i in reversed(range(1000)))
)

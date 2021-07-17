import math
from contextlib import contextmanager
from io import BytesIO
from operator import itemgetter
from struct import pack
from typing import Iterable, Tuple


def pwrite(b: BytesIO, offset: int, data: bytes) -> None:
    old_tell = b.tell()
    b.seek(offset)
    b.write(data)
    b.seek(old_tell)


@contextmanager
def write_size(b: BytesIO, *, pack_format: str) -> None:
    size_offset = b.tell()
    b.write(pack(pack_format, 0))  # will be filled later
    start_offset = b.tell()
    yield
    pwrite(b, size_offset, pack(pack_format, b.tell() - start_offset))


def get_btree_level(idx: int) -> int:
    # I don't have any mathematical proof to that (algorithm was deduced from
    # visual form and some experimentation), but I feel that something about
    # it is correct and it's working brilliantly.
    # "Level" here is the height from bottom at which the node with that index
    # will reside if there was a binary tree.
    # Basically, it is counting where the first "1" resides in the binary
    # number representation of `idx`.
    # The special case is for `idx` `0`. There is no "1" anywhere, so it's
    # assigned the special "0" level (by the name of `idx`).

    if idx == 0:
        return 0

    level = 1
    while idx % 2 == 0:
        idx >>= 1
        level += 1
    return level


def pack_tree(key_offsets: Iterable[Tuple[str, int]], write_to: BytesIO) -> None:
    write_node_offsets_to = {}
    node_offsets = []

    tree_base = write_to.tell()
    write_to.write(pack('<L', 2 ** 32 - 1))  # will be filled later

    idx = None
    prev_key = None
    for idx, (key, value_offset) in enumerate(key_offsets):
        if prev_key is not None:
            assert key > prev_key, f'Unsorted: {key} !> {prev_key}'
        prev_key = key

        # we need to reference that node from the others somehow,
        # so save the offset from the start of the Tree object
        offset = write_to.tell() - tree_base
        if (write_node_offset_to := write_node_offsets_to.pop(idx, None)) is not None:
            # we're requested to write our offset to some other node
            pwrite(write_to, write_node_offset_to, pack('<L', offset))
        node_offsets.append(offset)

        print(idx, f'({offset}, {key})', end='')

        # start writing the node
        with write_size(write_to, pack_format='<H'):
            # write key
            key = key.encode('utf-8')
            write_to.write(pack('<H', len(key)))
            write_to.write(key)

            # write value offset
            write_to.write(pack('<Q', value_offset))

            # write children node offsets (if needed)
            # for level = 0, there is no children
            # for level = 1, there is always only one children which will be
            #                written - "less than"
            # for level = 2 and up, there is one or two children which will be
            #               written - "less than" and "greater than"

            # we don't need to write the extra children because:
            #  1. it will save space
            #  2. it will prevent search algorithm from being cycled when key
            #     could not been found

            level = get_btree_level(idx)

            # `idx` `1` is special because although it on level `1`, it should
            # still reference `idx` `0` - otherwise the latter will not be
            # accessible from anywhere!
            if level > 1 or idx == 1:
                # the "delta" is a number of indexes to skip (both forward and
                # backwards) to access the children on the lower level
                # special case for level `1` - "delta" cannot be < 1
                delta = max(1, 2 ** (level - 2))

                # write left ("less than") child
                write_to.write(pack('<L', node_offsets[idx - delta]))
                print('', idx - delta, end='')

                # we need to write
                if level > 1:
                    # if there is no child directly underneath us,
                    # descend deeper
                    while idx + delta >= len(key_offsets):
                        delta //= 2

                    if delta:
                        # ask the children to write a reference to themself
                        # here when it will be packed (we don't know their
                        # offset until then)
                        assert (idx + delta) not in write_node_offsets_to
                        write_node_offsets_to[idx + delta] = write_to.tell()
                        write_to.write(pack('<L', 2 ** 32 - 1))
                        print('', idx + delta, end='')

        print()

    # write the root
    if idx is not None:
        # the "root" is the topmost level node (it can access any other node)
        root_idx = 2 ** (math.ceil(math.log(idx + 1, 2)) - 1)
        pwrite(write_to, tree_base, pack('<L', node_offsets[root_idx]))
    else:
        # empty tree
        pwrite(write_to, tree_base, pack('<L', 0))


def pack_into_file(filename: str, key_n_values: Iterable[Tuple[str, str]]):
    with open(filename, 'wb') as f:
        f.write(b'ROFL\x00\x00')

        # write the values first (to offload space to disk)
        key_n_offsets = []
        with write_size(f, pack_format='<Q'):
            value_base = f.tell()
            for key, value in key_n_values:
                value = value.encode('utf-8')
                value_offset = f.tell() - value_base
                f.write(pack('<L', len(value)))
                f.write(value)
                key_n_offsets.append((key, value_offset))
        del key_n_values

        # write the tree
        with write_size(f, pack_format='<L'):
            pack_tree(sorted(key_n_offsets, key=itemgetter(0)), f)


pack_into_file(
    'test_pack_python.rofldb',
    ((f'key{i}', f'value{i}') for i in reversed(range(1000)))
)

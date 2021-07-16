from __future__ import annotations

import bisect
import math
import queue
from io import BytesIO
from typing import Optional, Union

from itertools import chain, zip_longest
from struct import pack


class Leaf:
    def __init__(self, previous_leaf, next_leaf, parent, b_factor):
        self.previous = previous_leaf
        self.next = next_leaf
        self.parent = parent
        self.b_factor = b_factor
        self.a_factor = math.ceil(b_factor/2)
        self.keys = []
        self.children = []

    @property
    def is_root(self):
        return self.parent is None

    def insert(self, key, value):
        index = bisect.bisect_left(self.keys, key)
        if index < len(self.keys) and self.keys[index] == key:
            self.children[index].append(value)
        else:
            self.keys.insert(index, key)
            self.children.insert(index, [value])
            if len(self.keys) > self.b_factor:
                split_index = math.ceil(self.b_factor/2)
                self.split(split_index)

    def get(self, key):
        index = bisect.bisect_left(self.keys, key)
        if index < len(self.keys) and self.keys[index] == key:
            return self.children[index]
        else:
            return None

    def split(self, index):
        new_leaf_node = Leaf(self, self.next, self.parent, self.b_factor)
        new_leaf_node.keys = self.keys[index:]
        new_leaf_node.children = self.children[index:]
        self.keys = self.keys[:index]
        self.children = self.children[:index]
        if self.next is not None:
            self.next.previous = new_leaf_node
        self.next = new_leaf_node
        if self.is_root:
            self.parent = Node(None, None, [new_leaf_node.keys[0]], [self, self.next], b_factor=self.b_factor, parent=None)
            self.next.parent = self.parent
        else:
            self.parent.add_child(self.next.keys[0], self.next)

    def find_left(self, key, include_key=True):
        index = bisect.bisect_right(self.keys, key) - 1
        if index == -1:
            items = []
        else:
            if include_key:
                items = self.children[:index+1]
            else:
                if key == self.keys[index]:
                    index -= 1
                items = self.children[:index+1]
        return self.left_items() + chain.from_iterable(items)

    def find_right(self, key, include_key=True):
        index = bisect.bisect_left(self.keys, key)
        if index == len(self.keys):
            items = []
        else:
            if include_key:
                items = self.children[index:]
            else:
                if key == self.keys[index]:
                    index += 1
                items = self.children[index:]
        return chain.from_iterable(items) + self.right_items()

    def left_items(self):
        items = []
        node = self
        while node.previous is not None:
            node = node.previous
        while node != self:
            for elem in node.children:
                if type(elem) == list:
                    items.extend(elem)
                else:
                    items.append(elem)
            node = node.next
        return items

    def right_items(self):
        items = []
        node = self.next
        while node is not None:
            for elem in node.children:
                if type(elem) == list:
                    items.extend(elem)
                else:
                    items.append(elem)
            node = node.next
        return items

    def items(self):
        return zip(self.keys, self.children)


class Node:
    def __init__(self, previous_node, next_node, keys, children, b_factor, parent=None):
        self.previous = previous_node
        self.next = next_node
        self.keys = keys
        self.children = children
        self.b_factor = b_factor
        self.a_factor = math.ceil(b_factor / 2)
        self.parent = parent

    @property
    def degree(self):
        return len(self.children)

    @property
    def is_root(self):
        return self.parent is None

    def insert(self, key, value):
        index = bisect.bisect_right(self.keys, key)
        node = self.children[index]
        node.insert(key, value)

    def get(self, key):
        index = bisect.bisect_right(self.keys, key)
        return self.children[index].get(key)

    def find_left(self, key, include_key=True):
        index = bisect.bisect_right(self.keys, key)
        return self.children[index].find_left(key, include_key)

    def find_right(self, key, include_key=True):
        index = bisect.bisect_right(self.keys, key)
        return self.children[index].find_right(key, include_key)

    def add_child(self, key, child):
        index = bisect.bisect_right(self.keys, key)
        self.keys.insert(index, key)
        self.children.insert(index+1, child)
        if self.degree > self.b_factor:
            split_index = math.floor(self.b_factor / 2)
            self.split(split_index)

    def split(self, index):
        split_key = self.keys[index]
        new_node = Node(self, self.next, self.keys[index+1:], self.children[index+1:], self.b_factor, self.parent)
        for node in self.children[index+1:]:
            node.parent = new_node
        self.keys = self.keys[:index]
        self.children = self.children[:index+1]

        if self.next is not None:
            self.next.previous = new_node
        self.next = new_node
        if self.is_root:
            self.parent = Node(None, None, [split_key], [self, self.next], b_factor=self.b_factor, parent=None)
            self.next.parent = self.parent
        else:
            self.parent.add_child(split_key, self.next)


class BPlusTree:
    def __init__(self, b_factor=32):
        self.b_factor = b_factor
        self.root = Leaf(None, None, None, b_factor)
        self.size = 0

    def get(self, key):
        return self.root.get(key)

    def __getitem__(self, key):
        return self.get(key)

    def __len__(self):
        return self.size

    def insert(self, key, value):
        self.root.insert(key, value)
        self.size += 1
        if self.root.parent is not None:
            self.root = self.root.parent

    def range_search(self, notation, cmp_key):
        notation = notation.strip()
        if notation not in [">", "<", ">=", "<="]:
            raise Exception("Nonsupport notation: {}. Only '>' '<' '>=' '<=' are supported".format(notation))
        if notation == '>':
            return self.root.find_right(cmp_key, False)
        if notation == '>=':
            return self.root.find_right(cmp_key, True)
        if notation == '<':
            return self.root.find_left(cmp_key, False)
        if notation == '<=':
            return self.root.find_left(cmp_key, True)

    def search(self, notation, cmp_key):
        notation = notation.strip()
        if notation not in [">", "<", ">=", "<=", "==", "!="]:
            raise Exception("Nonsupport notation: {}. Only '>' '<' '>=' '<=' '==' '!=' are supported".format(notation))
        if notation == '==':
            res = self.get(cmp_key)
            if res is None:
                return []
            else:
                return res
        if notation == '!=':
            return self.root.find_left(cmp_key, False) + self.root.find_right(cmp_key, False)
        return self.range_search(notation, cmp_key)

    def show(self):
        layer = 0
        node = self.root
        while node is not None:
            print("Layer: {}".format(layer))
            inner_node = node
            while inner_node is not None:
                print(inner_node.keys, end=' ')
                inner_node = inner_node.next
            print('')
            node = node.children[0]
            layer += 1
            if type(node) != Leaf and type(node) != Node:
                break

    def leftmost_leaf(self):
        leaf = self.root
        while type(leaf) != Leaf:
            leaf = leaf.children[0]
        return leaf

    def items(self):
        leaf = self.leftmost_leaf()
        items = []
        while leaf is not None:
            pairs = list(leaf.items())
            items.extend(pairs)
            leaf = leaf.next
        return items

    def keys(self):
        leaf = self.leftmost_leaf()
        ks = []
        while leaf is not None:
            ks.extend(leaf.keys)
            leaf = leaf.next
        return ks

    def values(self):
        leaf = self.leftmost_leaf()
        vals = []
        while leaf is not None:
            for elem in leaf.children:
                if type(elem) == list:
                    vals.extend(elem)
                else:
                    vals.append(elem)
            leaf = leaf.next
        return vals

    def height(self):
        node = self.root
        height = 0
        while type(node) != Leaf:
            height += 1
            node = node.children[0]
        return height


class PriorityEntry:
    def __init__(self, priority, data: Union[Node, Leaf]):
        self.priority = priority
        self.data = data

    def __lt__(self, other):
        return self.priority < other.priority


def pwrite(f, offset: int, data: bytes):
    old_tell = f.tell()
    f.seek(offset)
    f.write(data)
    f.seek(old_tell)


def pack_tree(tree: BPlusTree):
    data_b = BytesIO()

    pack_queue = queue.PriorityQueue()

    def add_node_to_pack_queue(node: Union[Node, Leaf], *, write_offset_to: Optional[int] = None):
        pack_queue.put(PriorityEntry(isinstance(node, Leaf), (node, write_offset_to)))

    with open('tree_gen.rofldb', 'wb') as f:
        f.write(b'ROFL\x00\x00')

        tree_size_offset = f.tell()
        f.write(pack('<L', 0))  # will be filled later
        tree_start_offset = f.tell()

        add_node_to_pack_queue(tree.root)

        while not pack_queue.empty():
            node: Union[Node, Leaf]
            node, write_offset_to = pack_queue.get().data

            if write_offset_to is not None:
                pwrite(f, write_offset_to, pack('<L', f.tell() - tree_start_offset))

            node_size_offset = f.tell()
            f.write(pack('<L', 0))
            node_start_offset = f.tell()

            print(node, node.keys, node.children)
            for key, value in zip_longest(node.keys, node.children):
                # write record: it's size, key length, key

                record_size_offset = f.tell()
                f.write(pack('<H', 0))
                record_start_offset = f.tell()

                if key:
                    f.write(pack('<H', len(key)) + key.encode('utf-8'))
                else:
                    f.write(pack('<H', 0))

                if isinstance(value, (Node, Leaf)):
                    f.write(b'\x01')
                    add_node_to_pack_queue(value, write_offset_to=f.tell())
                    f.write(pack('<L', 0))
                else:
                    assert len(value) == 1
                    value = value[0]
                    # write data: it's size and the data
                    data_offset = data_b.tell()
                    data_b.write(pack('<L', len(value)))
                    data_b.write(value.encode('utf-8'))
                    f.write(b'\x00')
                    f.write(pack('<Q', data_offset))

                pwrite(f, record_size_offset, pack('<H', f.tell() - record_start_offset))

            pwrite(f, node_size_offset, pack('<L', f.tell() - node_start_offset))

        tree_size = f.tell() - tree_start_offset

        f.write(pack('<Q', len(data_b.getbuffer())))
        f.write(data_b.getbuffer())

        f.seek(tree_size_offset)
        f.write(pack('<L', tree_size))


ORDER = 3

tree = BPlusTree(ORDER)
for i in range(4):
    tree.insert(f'key{i}', f'value{i}')

pack_tree(tree)

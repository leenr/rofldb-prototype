import queue
from io import BytesIO
from itertools import zip_longest
from struct import pack


class Node:
    """Base node object.
    Each node stores keys and values. Keys are not unique to each value, and as such values are
    stored as a list under each key.
    Attributes:
        order (int): The maximum number of keys each node can hold.
    """
    def __init__(self, order):
        """Child nodes can be converted into parent nodes by setting self.leaf = False. Parent nodes
        simply act as a medium to traverse the tree."""
        self.order = order
        self.keys = []
        self.values = []
        self.leaf = True

    def add(self, key, value):
        """Adds a key-value pair to the node."""
        # If the node is empty, simply insert the key-value pair.
        if not self.keys:
            self.keys.append(key)
            self.values.append(value)
            return None

        for i, item in enumerate(self.keys):
            # If new key matches existing key, replace the value.
            if key == item:
                self.values[i] = value
                break

            # If new key is smaller than existing key, insert new key to the left of existing key.
            elif key < item:
                self.keys = self.keys[:i] + [key] + self.keys[i:]
                self.values = self.values[:i] + [value] + self.values[i:]
                break

            # If new key is larger than all existing keys, insert new key to the right of all
            # existing keys.
            elif i + 1 == len(self.keys):
                self.keys.append(key)
                self.values.append(value)

    def split(self):
        """Splits the node into two and stores them as child nodes."""
        left = Node(self.order)
        right = Node(self.order)
        mid = self.order // 2

        left.keys = self.keys[:mid]
        left.values = self.values[:mid]

        right.keys = self.keys[mid:]
        right.values = self.values[mid:]

        # When the node is split, set the parent key to the left-most key of the right child node.
        self.keys = [right.keys[0]]
        self.values = [left, right]
        self.leaf = False

    def is_full(self):
        """Returns True if the node is full."""
        return len(self.keys) == self.order

    def show(self, counter=0):
        """Prints the keys at each level."""
        print(counter, str(self.keys))

        # Recursively print the key of child nodes (if these exist).
        if not self.leaf:
            for item in self.values:
                item.show(counter + 1)


class BPlusTree(object):
    """B+ tree object, consisting of nodes.
    Nodes will automatically be split into two once it is full. When a split occurs, a key will
    'float' upwards and be inserted into the parent node to act as a pivot.
    Attributes:
        order (int): The maximum number of keys each node can hold.
    """
    def __init__(self, order=8):
        self.root = Node(order)

    def _find(self, node, key):
        """ For a given node and key, returns the index where the key should be inserted and the
        list of values at that index."""
        for i, item in enumerate(node.keys):
            if key < item:
                return node.values[i], i

        return node.values[i + 1], i + 1

    def _merge(self, parent, child, index):
        """For a parent and child node, extract a pivot from the child to be inserted into the keys
        of the parent. Insert the values from the child into the values of the parent.
        """
        parent.values.pop(index)
        pivot = child.keys[0]

        for i, item in enumerate(parent.keys):
            if pivot < item:
                parent.keys = parent.keys[:i] + [pivot] + parent.keys[i:]
                parent.values = parent.values[:i] + child.values + parent.values[i:]
                break

            elif i + 1 == len(parent.keys):
                parent.keys += [pivot]
                parent.values += child.values
                break

    def insert(self, key, value):
        """Inserts a key-value pair after traversing to a leaf node. If the leaf node is full, split
        the leaf node into two.
        """
        parent = None
        child = self.root

        # Traverse tree until leaf node is reached.
        while not child.leaf:
            parent = child
            child, index = self._find(child, key)

        child.add(key, value)

        # If the leaf node is full, split the leaf node into two.
        if child.is_full():
            child.split()

            # Once a leaf node is split, it consists of a internal node and two leaf nodes. These
            # need to be re-inserted back into the tree.
            if parent and not parent.is_full():
                self._merge(parent, child, index)

    def retrieve(self, key):
        """Returns a value for a given key, and None if the key does not exist."""
        child = self.root

        while not child.leaf:
            child, index = self._find(child, key)

        for i, item in enumerate(child.keys):
            if key == item:
                return child.values[i]

        return None

    def show(self):
        """Prints the keys at each level."""
        self.root.show()


class PriorityEntry(object):
    def __init__(self, priority, data):
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

    with open('tree_gen.rofldb', 'wb') as f:
        f.write(b'ROFL\x00\x00')

        tree_size_offset = f.tell()
        f.write(pack('<L', 0))  # will be filled later
        tree_start_offset = f.tell()

        pack_queue = queue.PriorityQueue()
        pack_queue.put(PriorityEntry(tree.root.leaf, (tree.root, None)))

        while not pack_queue.empty():
            node, write_offset_to = pack_queue.get().data

            if write_offset_to is not None:
                pwrite(f, write_offset_to, pack('<L', f.tell() - tree_start_offset))

            node_size_offset = f.tell()
            f.write(pack('<L', 0))
            node_start_offset = f.tell()

            print(node, list(zip_longest(node.keys, node.values)))
            for key, value in zip_longest(node.keys, node.values):
                # write record: it's size, key length, key

                record_size_offset = f.tell()
                f.write(pack('<H', 0))
                record_start_offset = f.tell()

                if key:
                    f.write(pack('<H', len(key)) + key.encode('utf-8'))
                else:
                    f.write(pack('<H', 0))

                if node.leaf:
                    # write data: it's size and the data
                    data_offset = data_b.tell()
                    data_b.write(pack('<L', len(value)))
                    data_b.write(value.encode('utf-8'))
                    f.write(b'\x00')
                    f.write(pack('<Q', data_offset))
                else:
                    f.write(b'\x01')
                    pack_queue.put(PriorityEntry(value.leaf, (value, f.tell())))
                    f.write(pack('<L', 0))

                pwrite(f, record_size_offset, pack('<H', f.tell() - record_start_offset))

            pwrite(f, node_size_offset, pack('<L', f.tell() - node_start_offset))

        tree_size = f.tell() - tree_start_offset

        f.write(pack('<Q', len(data_b.getbuffer())))
        f.write(data_b.getbuffer())

        f.seek(tree_size_offset)
        f.write(pack('<L', tree_size))


ORDER = 2

tree = BPlusTree(ORDER)
for i in range(4):
    tree.insert(f'key{i}', f'value{i}')
# tree.show()

pack_tree(tree)

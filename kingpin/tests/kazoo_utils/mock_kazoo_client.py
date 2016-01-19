#!/usr/bin/python
#
# Copyright 2016 Pinterest, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Mock kazoo client"""

from functools import partial
from functools import wraps
import time

from collections import deque
import gevent

from kazoo.exceptions import ConnectionLossException
from kazoo.exceptions import NoNodeException
from kazoo.exceptions import NotEmptyException
from kazoo.handlers.gevent import SequentialGeventHandler
from kazoo.protocol.states import KazooState
from kazoo.protocol.states import ZnodeStat
from kazoo.recipe.party import Party
from kazoo.recipe.party import ShallowParty
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.recipe.watchers import DataWatch
from kazoo.retry import KazooRetry


def throws_when_stopped(f):
    """Decorator to throw ConnectionLossException when the client is stopped.

    This decorator expects the first argument to be the underlying client.

    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        if not args[0].connected():
            raise ConnectionLossException("underlying connection closed")
        return f(*args, **kwargs)

    return wrapper


class MockZnode(object):
    """Mock Zookeeper znodes."""

    def __init__(self, parent, name, value="", ephemeral=False):
        """Constructor.

        Args:
            parent: The parent znode, cannot be ephemeral znode.
            name: The name of this znode.
            value: The data of this znode, currently not in use.
            ephemeral: Whether this znode is ephemeral.

        """
        # Ephemeral node cannot have children
        if parent:
            assert not parent._ephemeral
        self._parent = parent
        self._name = name
        if not isinstance(value, basestring):
            raise TypeError("data must be a string")
        self._value = value
        self._ephemeral = ephemeral
        self._children = {}
        self._version = 0

    def __str__(self):
        nodes = []
        curr_node = self
        while curr_node:
            nodes.append(curr_node._name)
            curr_node = curr_node._parent
        nodes.reverse()
        if len(nodes) == 1:
            return "/"
        return '/'.join(nodes)

    def __key(self):
        return self._parent, self._name

    def __eq__(self, other):
        assert isinstance(other, self.__class__)
        return self.__key() == other.__key()

    def __hash__(self):
        return hash(self.__key())

    @property
    def name(self):
        return self._name

    @property
    def ephemeral(self):
        return self._ephemeral

    @property
    def children(self):
        return self._children.values()

    @property
    def parent(self):
        return self._parent

    def add_child(self, child_name, data, ephemeral=False):
        if "/" in child_name:
            raise RuntimeError("Znode name cannot contain /.")

        if child_name in self._children:
            return

        self._children[child_name] = MockZnode(
            self, child_name, value=data, ephemeral=ephemeral)

    def remove_child(self, child_name):
        self._children.pop(child_name)

    def has_child(self, child_name):
        return child_name in self._children

    def get_child(self, child_name):
        return self._children.get(child_name)

    def __get_value(self):
        return self._value

    def __set_value(self, value):
        self._value = value

    value = property(fget=__get_value, fset=__set_value)

    def __get_version(self):
        return self._version

    def __set_version(self, new_version):
        self._version = new_version

    version = property(fget=__get_version, fset=__set_version)

    @property
    def stat(self):
        return ZnodeStat(
            czxid=0, mzxid=0, ctime=time.time(), mtime=time.time(),
            version=self.version, cversion=0, aversion=0, ephemeralOwner=0,
            dataLength=len(self._value), numChildren=len(self._children),
            pzxid=0)


class MockKazooClient(object):
    """Mock kazoo client.

    Right now, only mock as much as the tests requires.

    The mock kazoo client is only compatible with Kazoo 1.0.

    """

    # make this the class member so that the non-ephemeral znode can last
    # across different clients.
    ZK_ROOT_NODE = MockZnode(None, "")

    def __init__(self, handler=SequentialGeventHandler(), hosts=None):
        self.handler = handler
        self.hosts = hosts
        self._state = KazooState.LOST
        self._listeners = []
        self.Party = partial(Party, self)
        self.ShallowParty = partial(ShallowParty, self)
        self.retry = KazooRetry(
            max_tries=3,
            delay=0.0,
            backoff=1,
            max_jitter=0.0,
            sleep_func=gevent.sleep
        )
        self.ChildrenWatch = partial(ChildrenWatch, self)
        self.DataWatch = partial(DataWatch, self)
        self._children_watches = {}
        self._data_watches = {}

    def start(self, timeout=20):
        self._make_state_change(KazooState.CONNECTED)

    def stop(self):
        """All the ephemeral nodes and watches will be removed."""
        self._remove_all_ephemeral_nodes()
        self._children_watches = {}
        self._data_watches = {}

        self._make_state_change(KazooState.LOST)

    def _remove_all_ephemeral_nodes(self):
        """Traverse the tree to remove all ephemeral nodes."""
        nodes = deque([MockKazooClient.ZK_ROOT_NODE])
        while nodes:
            curr_node = nodes.popleft()
            if curr_node.ephemeral:
                # Ephemeral nodes cannot have children
                curr_node.parent.remove_child(curr_node.name)
            else:
                for child in curr_node.children:
                    nodes.append(child)

    def _make_state_change(self, state):
        if state == self._state:
            return
        self._state = state
        for listener in self._listeners:
            listener(state)

    def connected(self):
        """Whether this client is connected."""
        return self._state == KazooState.CONNECTED

    def _get_node_names(self, path):
        node_names = path.split("/")
        if path.endswith("/"):
            node_names = node_names[:-1]
        return node_names

    @throws_when_stopped
    def ensure_path(self, path):
        """Ensure the existence of the path; if not, create the nodes."""
        if not path.startswith("/"):
            raise RuntimeError("Path has to be absolute.")

        parent_node = None
        first_non_existing_node = True
        for node_name in self._get_node_names(path):
            if parent_node is None:
                # The root node, name should be empty string
                assert not node_name
                parent_node = MockKazooClient.ZK_ROOT_NODE
            else:
                tmp_parent_node = parent_node.get_child(node_name)
                if tmp_parent_node is None:
                    parent_node.add_child(node_name, "")
                    tmp_parent_node = parent_node.get_child(node_name)
                    if first_non_existing_node:
                        self._notify_children_watches(str(parent_node))
                        first_non_existing_node = False
                parent_node = tmp_parent_node

    def _find(self, path):
        parent_node = None
        for node_name in self._get_node_names(path):
            if parent_node is None:
                assert not node_name
                parent_node = MockKazooClient.ZK_ROOT_NODE
            else:
                parent_node = parent_node.get_child(node_name)
                if parent_node is None:
                    return None
        return parent_node

    def _register_children_watch(self, path, watch):
        if path not in self._children_watches:
            self._children_watches[path] = []
        self._children_watches[path].append(watch)

    def _notify_children_watches(self, path):
        children = self.get_children(path)
        watches = self._children_watches.get(path)
        # Clear out the watches, since they are supposed to fire only once
        self._children_watches[path] = []
        if watches:
            for watch in watches:
                watch(children)

    def _register_data_watch(self, path, watch):
        if path not in self._data_watches:
            self._data_watches[path] = []
        self._data_watches[path].append(watch)

    def _notify_data_watches(self, path, data):
        watches = self._data_watches.get(path)
        # Clear out the watches, since they are supposed to fire only once
        self._data_watches[path] = []
        if watches:
            for watch in watches:
                watch(data)

    @throws_when_stopped
    def create(self, path, data="", ephemeral=False, makepath=False):
        """Create a znode.

        Args:
            path: The path to this node.
            data: The data associated with this node, currently not in use.
            ephemeral: Whether this node is an ephemeral node or not.
            makepath: Whether to make the path if not existing already. Set
                to True, if creating all the znodes along the path if not
                existing is desired.

        Throws:
            NoNodeException, when makepath is False, and the parent node
            doesn't already exist.

        """
        if not path.startswith("/"):
            raise RuntimeError("Path has to be absolute.")

        parent_path = path[:path.rfind("/")]
        child_name = path[(path.rfind("/") + 1):]

        parent_node = self._find(parent_path)

        ensure_path_called = False
        if parent_node is None:
            if not makepath:
                raise NoNodeException("%s doesn't exist", parent_path)
            else:
                self.ensure_path(parent_path)
                parent_node = self._find(parent_path)
                ensure_path_called = True

        assert not parent_node.ephemeral
        parent_node.add_child(child_name, data, ephemeral=ephemeral)
        if not ensure_path_called:
            # Otherwise, ensure_path would have sent notification
            self._notify_children_watches(str(parent_node))

    @throws_when_stopped
    def get(self, path, watch=None):
        """Get the value of a node.

        If a watch is provided, it will be left on the node with the
        given path. The watch will be triggered by a successful
        operation that sets data on the node, or deletes the node.

        Args:
            path: Path of node.
            watch: Optional watch callback to set for future changes
                to this path.

        Returns:
            A tuple (value, `kazoo.protocol.states.ZnodeStat`) of node.

        Raises:
            `kazoo.exceptions.NoNodeError` if the node doesn't exist.

        """
        if not isinstance(path, basestring):
            raise TypeError("path must be a string")
        if watch and not callable(watch):
            raise TypeError("watch must be a callable")
        if not self.exists(path):
            raise NoNodeException("Node %s doesn't exist." % path)
        znode = self._find(path)
        if watch is not None:
            self._register_data_watch(path, watch)
        return znode.value, znode.stat

    @throws_when_stopped
    def set(self, path, data, version=-1):
        if not path.startswith("/"):
            raise RuntimeError("Path has to be absolute.")

        if not isinstance(data, basestring):
            raise TypeError("data must be a string")

        if not self.exists(path):
            raise NoNodeException("Node %s doesn't exist." % path)

        znode = self._find(path)
        znode.value = data
        znode.version += 1

        self._notify_data_watches(path, znode.value)

    @throws_when_stopped
    def exists(self, path):
        return self._find(path) is not None

    @throws_when_stopped
    def delete(self, path, recursive=False):
        """Delete a znode and optionally recursively delete its children.

        Args:
            path: The path to the znode to be deleted.
            recursive: Recursively delete the children znode instead of
                failing the request if there are children attached.

        Throws:
            NotEmptyException, if recursive is false, and there are children
            nodes attached.

        """
        # Removing the root is not allowed
        assert not path == "/"

        node = self._find(path)
        if node is None:
            return NotEmptyException("%s not empty" % path)
        if node.children and not recursive:
            raise
        node.parent.remove_child(node.name)
        self._notify_children_watches(str(node.parent))

    @throws_when_stopped
    def get_children(self, path, watch=None):
        """Get the children znode names.

        Args:
            path: The path to the parent znode.
            watch: The watch callback if the children changed after this call.

        Returns:
            A list of the children znode names.

        """
        node = self._find(path)
        if node is None:
            return None
        if watch:
            # Currently the only place to register children watch
            self._register_children_watch(path, watch)
        return (child.name for child in node.children)

    def add_listener(self, listener):
        """Add listener callback for connection state transition."""
        self._listeners.append(listener)

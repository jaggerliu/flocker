# -*- test-case-name: flocker.node.agents.test.test_blockdevice -*-
# Copyright Hybrid Logic Ltd.  See LICENSE file for details.

"""
This module implements the parts of a block-device based dataset
convergence agent that can be re-used against many different kinds of block
devices.
"""

from uuid import uuid4
from subprocess import check_output

from eliot import ActionType, Field, Logger

from zope.interface import implementer, Interface

from pyrsistent import PRecord, field

from twisted.internet.defer import succeed
from twisted.python.filepath import FilePath

from .. import IDeployer, IStateChange, InParallel
from ...control import Node, NodeState, Manifestation, Dataset


class VolumeException(Exception):
    """
    A base class for exceptions raised by  ``IBlockDeviceAPI`` operations.

    :param unicode blockdevice_id: The unique identifier of the block device.
    """
    def __init__(self, blockdevice_id):
        if not isinstance(blockdevice_id, unicode):
            raise TypeError(
                'Unexpected blockdevice_id type. '
                'Expected unicode. '
                'Got {!r}.'.format(blockdevice_id)
            )
        Exception.__init__(self, blockdevice_id)
        self.blockdevice_id = blockdevice_id


class UnknownVolume(VolumeException):
    """
    The block device could not be found.
    """


class AlreadyAttachedVolume(VolumeException):
    """
    A failed attempt to attach a block device that is already attached.
    """


class UnattachedVolume(VolumeException):
    """
    An attempt was made to operate on an unattached volume but the operation
    requires the volume to be attached.
    """


DATASET = Field(
    u"dataset",
    lambda dataset: dataset.dataset_id,
    u"The unique identifier of a dataset."
)

MOUNTPOINT = Field(
    u"mountpoint",
    lambda path: path.path,
    u"The absolute path to the location on the node where the dataset will be "
    u"mounted.",
)

DEVICE = Field(
    u"block_device",
    lambda path: path.path,
    u"The absolute path to the block device file on the node where the "
    u"dataset is attached.",
)

CREATE_BLOCK_DEVICE_DATASET = ActionType(
    u"agent:blockdevice:create",
    [DATASET, MOUNTPOINT],
    [DEVICE],
    u"A block-device-backed dataset is being created.",
)


@implementer(IStateChange)
class CreateBlockDeviceDataset(PRecord):
    """
    An operation to create a new dataset on a newly created volume with a newly
    initialized filesystem.

    :ivar Dataset dataset: The dataset for which to create a block device.
    :ivar FilePath mountpoint: The path at which to mount the created device.
    :ivar Logger logger: An Eliot ``Logger``.
    """
    dataset = field()
    mountpoint = field(mandatory=True)

    logger = Logger()

    def run(self, deployer):
        """
        Create a block device, attach it to the host of the supplied
        ``deployer``, create an ``ext4`` filesystem on the device and mount it.

        Operations are performed synchronously.

        See ``IStateChange.run`` for general argument and return type
        documentation.

        :returns: An already fired ``Deferred`` with result ``None``.
        """
        with CREATE_BLOCK_DEVICE_DATASET(
                self.logger,
                dataset=self.dataset, mountpoint=self.mountpoint
        ) as action:
            api = deployer.block_device_api
            volume = api.create_volume(
                self.dataset.maximum_size
            )
            volume = api.attach_volume(
                volume.blockdevice_id, deployer.hostname.encode('ascii')
            )
            device = api.get_device_path(volume.blockdevice_id)
            self.mountpoint.makedirs()
            check_output(["mkfs", "-t", "ext4", device.path])
            check_output(["mount", device.path, self.mountpoint.path])
            action.add_success_fields(block_device=device)
        return succeed(None)


# TODO: Introduce a non-blocking version of this interface and an automatic
# thread-based wrapper for adapting this to the other.  Use that interface
# anywhere being non-blocking is important (which is probably lots of places).
# See https://clusterhq.atlassian.net/browse/FLOC-1549
class IBlockDeviceAPI(Interface):
    """
    Common operations provided by all block device backends.

    Note: This is an early sketch of the interface and it'll be refined as we
    real blockdevice providers are implemented.
    """
    def create_volume(size):
        """
        Create a new block device.

        XXX: Probably needs to be some checking of valid sizes for different
        backends. Perhaps the allowed sizes should be defined as constants?

        :param int size: The size of the new block device in bytes.
        :returns: A ``BlockDeviceVolume``.
        """

    def attach_volume(blockdevice_id, host):
        """
        Attach ``blockdevice_id`` to ``host``.

        :param unicode blockdevice_id: The unique identifier for the block
            device being attached.
        :param unicode host: The IP address of a host to attach the volume to.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises AlreadyAttachedVolume: If the supplied ``blockdevice_id`` is
            already attached.
        :returns: A ``BlockDeviceVolume`` with a ``host`` attribute set to
            ``host``.
        """

    def list_volumes():
        """
        List all the block devices available via the back end API.

        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """

    def get_device_path(blockdevice_id):
        """
        Return the device path that has been allocated to the block device on
        the host to which it is currently attached.

        :param unicode blockdevice_id: The unique identifier for the block
            device.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to a host.
        :returns: A ``FilePath`` for the device.
        """


class BlockDeviceVolume(PRecord):
    """
    A block device that may be attached to a host.

    :ivar unicode blockdevice_id: The unique identifier of the block device.
    :ivar int size: The size, in bytes, of the block device.
    :ivar unicode host: The IP address of the host to which the block device is
        attached or ``None`` if it is currently unattached.
    """
    blockdevice_id = field(type=unicode, mandatory=True)
    size = field(type=int, mandatory=True)
    host = field(type=(unicode, type(None)), initial=None)


def _losetup_list_parse(output):
    """
    Parse the output of ``losetup --all`` which varies depending on the
    privileges of the user.

    :param unicode output: The output of ``losetup --all``.
    :returns: A ``list`` of
        2-tuple(FilePath(device_file), FilePath(backing_file))
    """
    devices = []
    for line in output.splitlines():
        parts = line.split(u":", 2)
        if len(parts) != 3:
            continue
        device_file, attributes, backing_file = parts
        device_file = FilePath(device_file.strip().encode("utf-8"))

        # Trim everything from the first left bracket, skipping over the
        # possible inode number which appears only when run as root.
        left_bracket_offset = backing_file.find(b"(")
        backing_file = backing_file[left_bracket_offset + 1:]

        # Trim everything from the right most right bracket
        right_bracket_offset = backing_file.rfind(b")")
        backing_file = backing_file[:right_bracket_offset]

        # Trim a possible embedded deleted flag
        expected_suffix_list = [b"(deleted)"]
        for suffix in expected_suffix_list:
            offset = backing_file.rfind(suffix)
            if offset > -1:
                backing_file = backing_file[:offset]

        # Remove the space that may have been between the path and the deleted
        # flag.
        backing_file = backing_file.rstrip()
        backing_file = FilePath(backing_file.encode("utf-8"))
        devices.append((device_file, backing_file))
    return devices


def _losetup_list():
    """
    List all the loopback devices on the system.

    :returns: A ``list`` of
        2-tuple(FilePath(device_file), FilePath(backing_file))
    """
    output = check_output(
        ["losetup", "--all"]
    ).decode('utf8')
    return _losetup_list_parse(output)


def _device_for_path(expected_backing_file):
    """
    :param FilePath backing_file: A path which may be associated with a
        loopback device.
    :returns: A ``FilePath`` to the loopback device if one is found, or
        ``None`` if no device exists.
    """
    for device_file, backing_file in _losetup_list():
        if expected_backing_file == backing_file:
            return device_file


@implementer(IBlockDeviceAPI)
class LoopbackBlockDeviceAPI(object):
    """
    A simulated ``IBlockDeviceAPI`` which creates loopback devices backed by
    files located beneath the supplied ``root_path``.
    """
    _attached_directory_name = 'attached'
    _unattached_directory_name = 'unattached'

    def __init__(self, root_path):
        """
        :param FilePath root_path: The path beneath which all loopback backing
            files and their organising directories will be created.
        """
        self._root_path = root_path

    @classmethod
    def from_path(cls, root_path):
        """
        :param bytes root_path: The path to a directory in which loop back
            backing files will be created. The directory is created if it does
            not already exist.
        :returns: A ``LoopbackBlockDeviceAPI`` with the supplied ``root_path``.
        """
        api = cls(root_path=FilePath(root_path))
        api._initialise_directories()
        return api

    def _initialise_directories(self):
        """
        Create the root and sub-directories in which loopback files will be
        created.
        """
        self._unattached_directory = self._root_path.child(
            self._unattached_directory_name)

        try:
            self._unattached_directory.makedirs()
        except OSError:
            pass

        self._attached_directory = self._root_path.child(
            self._attached_directory_name)

        try:
            self._attached_directory.makedirs()
        except OSError:
            pass

    def create_volume(self, size):
        """
        Create a "sparse" file of some size and put it in the ``unattached``
        directory.

        See ``IBlockDeviceAPI.create_volume`` for parameter and return type
        documentation.
        """
        volume = BlockDeviceVolume(
            blockdevice_id=unicode(uuid4()),
            size=size,
        )
        with self._unattached_directory.child(
            volume.blockdevice_id.encode('ascii')
        ).open('wb') as f:
            f.truncate(size)
        return volume

    def _get(self, blockdevice_id):
        for volume in self.list_volumes():
            if volume.blockdevice_id == blockdevice_id:
                return volume
        raise UnknownVolume(blockdevice_id)

    def attach_volume(self, blockdevice_id, host):
        """
        Move an existing ``unattached`` file into a per-host directory and
        create a loopback device backed by that file.

        Note: Although `mkfs` can format files directly and `mount` can mount
        files directly (with the `-o loop` option), we want to simulate a real
        block device which will be allocated a real block device file on the
        host to which it is attached. This allows the consumer of this API to
        perform formatting and mount operations exactly the same as for a real
        block device.

        See ``IBlockDeviceAPI.attach_volume`` for parameter and return type
        documentation.
        """
        volume = self._get(blockdevice_id)
        if volume.host is None:
            old_path = self._unattached_directory.child(blockdevice_id)
            host_directory = self._attached_directory.child(
                host.encode("utf-8")
            )
            try:
                host_directory.makedirs()
            except OSError:
                pass

            new_path = host_directory.child(blockdevice_id)
            old_path.moveTo(new_path)

            # The --find option allocates the next available /dev/loopX device
            # name to the device.
            check_output(["losetup", "--find", new_path.path])

            attached_volume = volume.set(host=host)
            return attached_volume

        raise AlreadyAttachedVolume(blockdevice_id)

    def list_volumes(self):
        """
        Return ``BlockDeviceVolume`` instances for all the files in the
        ``unattached`` directory and all per-host directories.

        See ``IBlockDeviceAPI.list_volumes`` for parameter and return type
        documentation.
        """
        volumes = []
        for child in self._root_path.child('unattached').children():
            volume = BlockDeviceVolume(
                blockdevice_id=child.basename().decode('ascii'),
                size=child.getsize(),
            )
            volumes.append(volume)

        for host_directory in self._root_path.child('attached').children():
            host_name = host_directory.basename().decode('ascii')
            for child in host_directory.children():

                volume = BlockDeviceVolume(
                    blockdevice_id=child.basename().decode('ascii'),
                    size=child.getsize(),
                    host=host_name,
                )
                volumes.append(volume)

        return volumes

    def get_device_path(self, blockdevice_id):
        volume = self._get(blockdevice_id)
        if volume.host is None:
            raise UnattachedVolume(blockdevice_id)

        volume_path = self._attached_directory.descendant(
            [volume.host, volume.blockdevice_id]
        )
        # May be None if the file hasn't been used for a loop device.
        return _device_for_path(volume_path)


def _manifestation_from_volume(volume):
    """
    :param BlockDeviceVolume volume: The block device which has the
        manifestation of a dataset.
    :returns: A primary ``Manifestation`` of a ``Dataset`` with the same id as
        the supplied ``BlockDeviceVolume``.
    """
    dataset = Dataset(dataset_id=volume.blockdevice_id)
    return Manifestation(dataset=dataset, primary=True)


@implementer(IDeployer)
@attributes(["hostname", "block_device_api"])
class BlockDeviceDeployer(object):
    """
    An ``IDeployer`` that operates on ``IBlockDeviceAPI`` providers.

    :param bytes hostname: The IP address of the node that has this deployer.
    :param IBlockDeviceAPI block_device_api: The block device API that will be
        called upon to perform block device operations.
    :ivar FilePath _mountroot: The directory where block devices will be
        mounted.
    """
    _mountroot = FilePath(b"/flocker")

    def discover_local_state(self):
        volumes = self.block_device_api.list_volumes()

        manifestations = [_manifestation_from_volume(v)
                          for v in volumes
                          if v.host == self.hostname]

        paths = {}
        for manifestation in manifestations:
            dataset_id = manifestation.dataset.dataset_id
            mountpath = self._mountpath_for_manifestation(manifestation)
            paths[dataset_id] = mountpath

        state = NodeState(
            hostname=self.hostname,
            running=(),
            not_running=(),
            manifestations=manifestations,
            paths=paths
        )
        return succeed(state)

    def _mountpath_for_manifestation(self, manifestation):
        """
        Calculate a ``Manifestation`` mount point.

        :param Manifestation manifestation: The manifestation of a dataset that
            will be mounted.
        :returns: A ``FilePath`` of the mount point.
        """
        return self._mountroot.child(
            manifestation.dataset.dataset_id.encode("ascii")
        )

    def calculate_necessary_state_changes(self, local_state,
                                          desired_configuration,
                                          current_cluster_state):
        potential_configs = list(
            node for node in desired_configuration.nodes
            if node.hostname == self.hostname
        )
        if len(potential_configs) == 0:
            this_node_config = Node(hostname=None)
        else:
            [this_node_config] = potential_configs
        configured = set(this_node_config.manifestations.values())
        to_create = configured.difference(local_state.manifestations)

        # TODO check for non-None size on dataset; cannot create block devices
        # of unspecified size.
        creates = list(
            CreateBlockDeviceDataset(
                dataset=manifestation.dataset,
                mountpoint=self._mountpath_for_manifestation(manifestation)
            )
            for manifestation
            in to_create
        )
        return InParallel(changes=creates)

# Copyright Hybrid Logic Ltd.  See LICENSE file for details.

"""
Testing utilities for ``flocker.acceptance``.
"""

from os import environ
from pipes import quote as shell_quote
from socket import gaierror, socket
from subprocess import check_call, PIPE, Popen
from unittest import SkipTest, skipUnless
from yaml import safe_dump, safe_load

from twisted.internet.defer import succeed
from twisted.python.filepath import FilePath
from twisted.python.procutils import which

from pyrsistent import pmap

from ..control import (
    Application, AttachedVolume, DockerImage, Manifestation, Dataset,
    FlockerConfiguration
)
from flocker.testtools import loop_until

from flocker.provision._install import stop_cluster

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
    PYMONGO_INSTALLED = True
except ImportError:
    PYMONGO_INSTALLED = False

__all__ = [
    'assert_expected_deployment', 'flocker_deploy', 'get_nodes',
    'MONGO_APPLICATION', 'MONGO_IMAGE', 'get_mongo_application',
    'require_flocker_cli', 'get_node_state', 'create_application',
    'create_attached_volume'
    ]

# XXX This assumes that the desired version of flocker-cli has been installed.
# Instead, the testing environment should do this automatically.
# See https://clusterhq.atlassian.net/browse/FLOC-901.
require_flocker_cli = skipUnless(which("flocker-deploy"),
                                 "flocker-deploy not installed")

require_mongo = skipUnless(
    PYMONGO_INSTALLED, "PyMongo not installed")


# XXX The MONGO_APPLICATION will have to be removed because it does not match
# the tutorial yml files, and the yml should be testably the same:
# https://clusterhq.atlassian.net/browse/FLOC-947
MONGO_APPLICATION = u"mongodb-example-application"
MONGO_IMAGE = u"clusterhq/mongodb"


def get_mongo_application():
    """
    Return a new ``Application`` with a name and image corresponding to
    the MongoDB tutorial example:

    http://doc-dev.clusterhq.com/gettingstarted/tutorial/index.html
    """
    return Application(
        name=MONGO_APPLICATION,
        image=DockerImage.from_string(MONGO_IMAGE + u':latest'),
    )


def create_application(name, image, ports=frozenset(), volume=None,
                       links=frozenset(), environment=None, memory_limit=None,
                       cpu_shares=None):
    """
    Instantiate an ``Application`` with the supplied parameters and return it.
    """
    return Application(
        name=name, image=DockerImage.from_string(image + u':latest'),
        ports=ports, volume=volume, links=links, environment=environment,
        memory_limit=memory_limit, cpu_shares=cpu_shares
    )


def create_attached_volume(dataset_id, mountpoint, maximum_size=None,
                           metadata=pmap()):
    """
    Create an ``AttachedVolume`` instance with the supplied parameters and
    return it.

    :param unicode dataset_id: The unique identifier of the dataset of the
        attached volume.
    :param bytes mountpoint: The path at which the volume is attached.
    :param int maximum_size: An optional maximum size for the volume.

    :return: A new ``AttachedVolume`` instance referencing a primary
        manifestation of a dataset with the given unique identifier.
    """
    return AttachedVolume(
        manifestation=Manifestation(
            dataset=Dataset(
                dataset_id=dataset_id,
                maximum_size=maximum_size,
                metadata=metadata,
            ),
            primary=True,
        ),
        mountpoint=FilePath(mountpoint),
    )


def get_node_state(node):
    """
    Call flocker-reportstate on the specified node and return its output,
    as ``Application`` instances parsed via class ``FlockerConfiguration``.
    """
    yaml = run_SSH(22, 'root', node, [b"flocker-reportstate"], None)
    state = safe_load(yaml)
    return FlockerConfiguration(state).applications()


def run_SSH(port, user, node, command, input, key=None,
            background=False):
    """
    Run a command via SSH.

    :param int port: Port to connect to.
    :param bytes user: User to run the command as.
    :param bytes node: Node to run command on.
    :param command: Command to run.
    :type command: ``list`` of ``bytes``.
    :param bytes input: Input to send to command.
    :param FilePath key: If not None, the path to a private key to use.
    :param background: If ``True``, don't block waiting for SSH process to
         end or read its stdout. I.e. it will run "in the background".
         Also ensures remote process has pseudo-tty so killing the local SSH
         process will kill the remote one.

    :return: stdout as ``bytes`` if ``background`` is false, otherwise
        return the ``subprocess.Process`` object.
    """
    quotedCommand = ' '.join(map(shell_quote, command))
    command = [
        b'ssh',
        b'-p', b'%d' % (port,),
        ]

    if key is not None:
        command.extend([
            b"-i",
            key.path])

    if background:
        # Force pseudo-tty so that remote process exists when the ssh
        # client does:
        command.extend([b"-t", b"-t"])

    command.extend([
        b'@'.join([user, node]),
        quotedCommand
    ])
    if background:
        process = Popen(command, stdin=PIPE)
        process.stdin.write(input)
        return process
    else:
        process = Popen(command, stdout=PIPE, stdin=PIPE)

    result = process.communicate(input)
    if process.returncode != 0:
        raise Exception('Command Failed', command, process.returncode)

    return result[0]


def _clean_node(test_case, node):
    """
    Remove all containers and zfs volumes on a node, given the IP address of
    the node.

    :param test_case: The ``TestCase`` running this unit test.
    :param bytes node: The hostname or IP of the node.
    """
    clean_deploy = {u"version": 1,
                    u"nodes": {node.decode("ascii"): []}}
    clean_applications = {u"version": 1,
                          u"applications": {}}
    flocker_deploy(test_case, clean_deploy, clean_applications)

    # Without the below, deploying the same application with a data volume
    # twice fails. See the error given with the tutorial's yml files:
    #
    #   $ flocker-deploy volume-deployment.yml volume-application.yml
    #   $ ssh root@${NODE} docker ps -a -q # outputs an ID, ${ID}
    #   $ ssh root@${NODE} docker stop ${ID}
    #   $ ssh root@${NODE} docker rm ${ID}
    #   $ flocker-deploy volume-deployment.yml volume-application.yml
    #
    # http://doc-dev.clusterhq.com/advanced/cleanup.html#removing-zfs-volumes
    # A tool or flocker-deploy option to purge the state of a node does
    # not yet exist. See https://clusterhq.atlassian.net/browse/FLOC-682
    run_SSH(22, 'root', node, [b"zfs"] + [b"destroy"] + [b"-r"] +
            [b"flocker"], None)


def _stop_acceptance_cluster():
    """
    Stop the Flocker cluster configured for the accpetance tests.

    XXX https://clusterhq.atlassian.net/browse/FLOC-1563
    Flocker doesn't support using flocker-deploy along-side flocker-control and
    flocker-agent. Since flocker-deploy (in it's SSH using incarnation) is
    going away, we do the hack of stopping the cluster before running tests
    that use flocker-deploy. This introduces an order dependency on the
    acceptance test-suite.

    This also removes the environment variables associated with the cluster, so
    that tests attempting to use it will be skipped.
    """
    control_node = environ.pop("FLOCKER_ACCEPTANCE_CONTROL_NODE", None)
    agent_nodes_env_var = environ.pop("FLOCKER_ACCEPTANCE_AGENT_NODES", "")
    agent_nodes = filter(None, agent_nodes_env_var.split(':'))

    if control_node and agent_nodes:
        stop_cluster(control_node, agent_nodes)


def get_nodes(test_case, num_nodes):
    """
    Create or get ``num_nodes`` nodes with no Docker containers on them.

    This is an alternative to
    http://doc-dev.clusterhq.com/gettingstarted/tutorial/
    vagrant-setup.html#creating-vagrant-vms-needed-for-flocker

    XXX This pretends to be asynchronous because num_nodes Docker containers
    will be created instead to replace this in some circumstances, see
    https://clusterhq.atlassian.net/browse/FLOC-900

    XXX https://clusterhq.atlassian.net/browse/FLOC-1563
    This also stop flocker-control and flocker-agent on the nodes.

    :param test_case: The ``TestCase`` running this unit test.
    :param int num_nodes: The number of nodes to start up.

    :return: A ``Deferred`` which fires with a set of IP addresses.
    """

    nodes_env_var = environ.get("FLOCKER_ACCEPTANCE_NODES")

    if nodes_env_var is None:
        raise SkipTest(
            "Set acceptance testing node IP addresses using the " +
            "FLOCKER_ACCEPTANCE_NODES environment variable and a colon " +
            "separated list.")

    # Remove any empty strings, for example if the list has ended with a colon
    nodes = filter(None, nodes_env_var.split(':'))

    if len(nodes) < num_nodes:
        raise SkipTest("This test requires a minimum of {necessary} nodes, "
                       "{existing} node(s) are set.".format(
                           necessary=num_nodes, existing=len(nodes)))

    reachable_nodes = set()

    for node in nodes:
        sock = socket()
        try:
            can_connect = not sock.connect_ex((node, 22))
        except gaierror:
            can_connect = False
        finally:
            if can_connect:
                reachable_nodes.add(node)
            sock.close()

    if len(reachable_nodes) < num_nodes:
        unreachable_nodes = set(nodes) - reachable_nodes
        test_case.fail(
            "At least {min} node(s) must be running and reachable on port 22. "
            "The following node(s) are reachable: {reachable}. "
            "The following node(s) are not reachable: {unreachable}.".format(
                min=num_nodes,
                reachable=", ".join(str(node) for node in reachable_nodes),
                unreachable=", ".join(str(node) for node in unreachable_nodes),
            )
        )

    # Stop flocker-control and flocker-agent here, as by this point, we know
    # that we aren't skipping this test.
    _stop_acceptance_cluster()

    # Only return the desired number of nodes
    reachable_nodes = set(sorted(reachable_nodes)[:num_nodes])

    for node in reachable_nodes:
        _clean_node(test_case, node)
    return succeed(reachable_nodes)


def flocker_deploy(test_case, deployment_config, application_config):
    """
    Run ``flocker-deploy`` with given configuration files.

    :param test_case: The ``TestCase`` running this unit test.
    :param dict deployment_config: The desired deployment configuration.
    :param dict application_config: The desired application configuration.
    """
    temp = FilePath(test_case.mktemp())
    temp.makedirs()

    deployment = temp.child(b"deployment.yml")
    deployment.setContent(safe_dump(deployment_config))

    application = temp.child(b"application.yml")
    application.setContent(safe_dump(application_config))

    check_call([b"flocker-deploy", deployment.path, application.path])


def get_mongo_client(host, port=27017):
    """
    Returns a ``Deferred`` which fires with a ``MongoClient`` when one has been
    created.

    See http://api.mongodb.org/python/current/api/pymongo/mongo_client.html#
        pymongo.mongo_client.MongoClient
    for more parameter information.

    :param bytes host: Hostname or IP address of the instance to connect to.
    :param int port: Port number on which to connect.

    The tutorial says "If you get a connection refused error try again after a
    few seconds; the application might take some time to fully start up."
    and so here we wait until the client can be created.
    """
    def create_mongo_client():
        try:
            return MongoClient(host=host, port=port)
        except ConnectionFailure:
            return False

    d = loop_until(create_mongo_client)
    return d


def assert_expected_deployment(test_case, expected_deployment):
    """
    Assert that the expected set of ``Application`` instances on a set of
    nodes is the same as the actual set of ``Application`` instance on
    those nodes.

    The tutorial looks at Docker output, but the acceptance tests are
    intended to test high-level external behaviors. Since this is looking
    at the output of ``flocker-reportstate`` it merely verifies what
    Flocker believes the system configuration is, not the actual
    setup. The latter should be verified separately with additional tests
    for external side-effects (applications being available on ports,
    say).

    :param test_case: The ``TestCase`` running this unit test.
    :param dict expected_deployment: A mapping of IP addresses to set of
        ``Application`` instances expected on the nodes with those IP
        addresses.
    """
    for node, expected in expected_deployment.items():
        yaml = run_SSH(22, 'root', node, [b"flocker-reportstate"], None)
        state = safe_load(yaml)
        test_case.assertSetEqual(
            set(FlockerConfiguration(state).applications().values()),
            expected)

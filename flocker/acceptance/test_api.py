# Copyright Hybrid Logic Ltd.  See LICENSE file for details.

"""
Tests for the control service REST API.
"""

import socket

from signal import SIGINT
from os import kill
from uuid import uuid4
from json import dumps, loads

from twisted.trial.unittest import TestCase
from treq import get, post, content, delete, json_content
from characteristic import attributes

from .testtools import get_nodes, run_SSH
from ..testtools import loop_until, random_name

from ..control.httpapi import REST_API_PORT


def wait_for_api(hostname):
    """
    Wait until REST API is available.

    :param str hostname: The host where the control service is
         running.

    :return Deferred: Fires when REST API is available.
    """
    def api_available():
        try:
            s = socket.socket()
            s.connect((hostname, REST_API_PORT))
            return True
        except socket.error:
            return False
    return loop_until(api_available)


@attributes(['address', 'agents'])
class Node(object):
    """
    A record of a cluster node and its agents.

    :ivar bytes address: The IPv4 address of the node.
    :ivar list agents: A list of ``RemoteService`` instances for the
        convergence agent processes that have been started on this node.
    """


@attributes(['address', 'process'])
class RemoteService(object):
    """
    A record of a background SSH process and the node that it's running on.

    :ivar bytes address: The IPv4 address on which the service is running.
    :ivar Subprocess.Popen process: The running ``SSH`` process that is running
        the remote process.
    """


def close(process):
    """
    Kill a process.

    :param subprocess.Popen process: The process to be killed.
    """
    process.stdin.close()
    kill(process.pid, SIGINT)


def remote_service_for_test(test_case, address, command):
    """
    Start a remote process (via SSH) for a test and register a cleanup function
    to stop it when the test finishes.

    :param TestCase test_case: The test case instance on which to register
        cleanup operations.
    :param bytes address: The IPv4 address of the node on which to run
        ``command``.
    :param list command: The command line arguments to run remotely via SSH.
    :returns: A ``RemoteService`` instance.
    """
    service = RemoteService(
        address=address,
        process=run_SSH(
            port=22,
            user='root',
            node=address,
            command=command,
            input=b"",
            key=None,
            background=True
        )
    )
    test_case.addCleanup(close, service.process)
    return service


@attributes(['control_service', 'nodes'])
class Cluster(object):
    """
    A record of the control service and the nodes in a cluster for acceptance
    testing.

    :param RemoteService control_service: The remotely running
        ``flocker-control`` process.
    :param list nodes: The ``Node`` s in this cluster.
    """
    @property
    def base_url(self):
        """
        :returns: The base url for API requests to this cluster's control
            service.
        """
        return b"http://{}:{}/v1".format(
            self.control_service.address, REST_API_PORT
        )

    def datasets_state(self):
        """
        Return the actual dataset state of the cluster.

        :return: ``Deferred`` firing with a list of dataset dictionaries,
            the state of the cluster.
        """
        request = get(self.base_url + b"/state/datasets", persistent=False)
        request.addCallback(json_content)
        return request

    def wait_for_dataset(self, dataset_properties):
        """
        Poll the dataset state API until the supplied dataset exists.

        :param dict dataset_properties: The attributes of the dataset that
            we're waiting for.
        :returns: A ``Deferred`` which fires with a 2-tuple of ``Cluster`` and
            API response when a dataset with the supplied properties appears in
            the cluster.
        """
        def created():
            """
            Check the dataset state list for the expected dataset.
            """
            request = self.datasets_state()

            def got_body(body):
                # State listing doesn't have metadata or deleted, but does
                # have unpredictable path.
                expected_dataset = dataset_properties.copy()
                del expected_dataset['metadata']
                del expected_dataset['deleted']
                for dataset in body:
                    dataset.pop("path")
                return expected_dataset in body
            request.addCallback(got_body)
            return request

        waiting = loop_until(created)
        waiting.addCallback(lambda ignored: (self, dataset_properties))
        return waiting

    def create_dataset(self, dataset_properties):
        """
        Create a dataset with the supplied ``dataset_properties``.

        :param dict dataset_properties: The properties of the dataset to
            create.
        :returns: A ``Deferred`` which fires with a 2-tuple of ``Cluster`` and
            API response when a dataset with the supplied properties has been
            persisted to the cluster configuration.
        """
        request = post(
            self.base_url + b"/configuration/datasets",
            data=dumps(dataset_properties),
            headers={b"content-type": b"application/json"},
            persistent=False
        )

        request.addCallback(content)
        request.addCallback(loads)
        # Return cluster and API response
        request.addCallback(lambda response: (self, response))
        return request

    def update_dataset(self, dataset_id, dataset_properties):
        """
        Update a dataset with the supplied ``dataset_properties``.

        :param unicode dataset_id: The uuid of the dataset to be modified.
        :param dict dataset_properties: The properties of the dataset to
            create.
        :returns: A 2-tuple of (cluster, api_response)
        """
        request = post(
            self.base_url + b"/configuration/datasets/%s" % (
                dataset_id.encode('ascii'),
            ),
            data=dumps(dataset_properties),
            headers={b"content-type": b"application/json"},
            persistent=False
        )

        request.addCallback(content)
        request.addCallback(loads)
        # Return cluster and API response
        request.addCallback(lambda response: (self, response))
        return request

    def delete_dataset(self, dataset_id):
        """
        Delete a dataset.

        :param unicode dataset_id: The uuid of the dataset to be modified.

        :returns: A 2-tuple of (cluster, api_response)
        """
        request = delete(
            self.base_url + b"/configuration/datasets/%s" % (
                dataset_id.encode('ascii'),
            ),
            headers={b"content-type": b"application/json"},
            persistent=False
        )

        request.addCallback(json_content)
        # Return cluster and API response
        request.addCallback(lambda response: (self, response))
        return request

    def create_container(self, properties):
        """
        Create a container with the specified properties.

        :param dict properties: A ``dict`` mapping to the API request fields
            to create a container.

        :returns: A tuple of (cluster, api_response)
        """
        request = post(
            self.base_url + b"/configuration/containers",
            data=dumps(properties),
            headers={b"content-type": b"application/json"},
            persistent=False
        )

        request.addCallback(json_content)
        request.addCallback(lambda response: (self, response))
        return request


def cluster_for_test(test_case, node_addresses, agent_command):
    """
    Build a ``Cluster`` instance with the supplied ``node_addresses``.

    ``flocker-control`` is started on the node with the lowest address and with
    a blank database.

    ``agent_command`` is started on every node.

    The processes will be killed after each test.

    :param TestCase test_case: The test case instance on which to register
        cleanup operations.
    :param list node_address: The IPv4 addresses of the nodes in the cluster.
    :param bytes agent_command: The entry point for the agent to run on each
        node.
    :returns: A ``Cluster`` instance.
    """
    # Start servers; eventually we will have these already running on
    # nodes, but for now needs to be done manually.
    # https://clusterhq.atlassian.net/browse/FLOC-1383

    control_service = remote_service_for_test(
        test_case,
        sorted(node_addresses)[0],
        [b"flocker-control",
         b"--data-path",
         b"/tmp/flocker.acceptance.test_api.cluster_for_test.%s" % (
             random_name(),)]
    )

    # https://clusterhq.atlassian.net/browse/FLOC-1382
    nodes = []
    for node_address in node_addresses:
        agent_service = remote_service_for_test(
            test_case,
            node_address,
            [agent_command, node_address, control_service.address],
        )
        node = Node(
            address=node_address,
            agents=[agent_service]
        )
        nodes.append(node)

    return Cluster(control_service=control_service, nodes=nodes)


def create_cluster_and_wait_for_api(test_case, node_addresses, agent_command):
    """
    Build a ``Cluster`` instance with the supplied ``node_addresses``.

    :param TestCase test_case: The test case instance on which to register
        cleanup operations.
    :param list node_addresses: The IPv4 addresses of the nodes in the cluster.
    :param bytes agent_command: The entry point for the agent to run on each
        node.
    :returns: A ``Cluster`` instance.
    """
    cluster = cluster_for_test(test_case, node_addresses, agent_command)
    waiting = wait_for_api(cluster.control_service.address)
    api_ready = waiting.addCallback(lambda ignored: cluster)
    return api_ready


def wait_for_cluster(test_case, node_count, agent_command):
    """
    Build a ``Cluster`` instance with ``node_count`` nodes.

    :param TestCase test_case: The test case instance on which to register
        cleanup operations.
    :param list node_count: The number of nodes to create in the cluster.
    :param bytes agent_command: The entry point for the agent to run on each
        node.
    :returns: A ``Deferred`` which fires with a ``Cluster`` instance when the
        ``control_service`` is accepting API requests.
    """
    getting_nodes = get_nodes(test_case, node_count)

    getting_nodes.addCallback(
        lambda nodes: create_cluster_and_wait_for_api(
            test_case, nodes, agent_command
        )
    )

    return getting_nodes


class ContainerAPITests(TestCase):
    """
    Tests for the container API.
    """
    def test_create_container_with_ports(self):
        """
        Create a container including port mappings on a single-node cluster.
        """
        data = {
            u"name": "my_container",
            u"host": None,
            u"image": "clusterhq/flask:latest",
            u"ports": [{u"internal": 80, u"external": 8080}]
        }
        waiting_for_cluster = wait_for_cluster(
            test_case=self, node_count=1, agent_command='flocker-zfs-agent'
        )

        def create_container(cluster, data):
            data[u"host"] = cluster.nodes[0].address
            return cluster.create_container(data)

        d = waiting_for_cluster.addCallback(create_container, data)

        def check_result(result):
            response = result[1]

            def can_connect():
                s = socket.socket()
                conn = s.connect_ex((data[u"host"], 8080))
                return False if conn else True

            dl = loop_until(can_connect)
            self.assertEqual(response, data)
            return dl

        d.addCallback(check_result)
        return d

    def test_create_container_with_environment(self):
        """
        Create a container including environment variables on a single-node
        cluster.
        """
        data = {
            u"name": "my_env_container",
            u"host": None,
            u"image": "clusterhq/flaskenv:latest",
            u"ports": [{u"internal": 8080, u"external": 8080}],
            u"environment": {u"ACCEPTANCE_ENV_LABEL": 'acceptance test ok'}
        }
        waiting_for_cluster = wait_for_cluster(
            test_case=self, node_count=1, agent_command='flocker-zfs-agent'
        )

        def create_container(cluster, data):
            data[u"host"] = cluster.nodes[0].address
            return cluster.create_container(data)

        d = waiting_for_cluster.addCallback(create_container, data)

        def check_result((cluster, response)):
            self.assertEqual(response, data)

        def verify_socket(host, port):
            def can_connect():
                s = socket.socket()
                conn = s.connect_ex((host, port))
                return False if conn else True

            dl = loop_until(can_connect)
            return dl

        def query_environment(host, port):
            """
            The running container, clusterhq/flaskenv, is a simple Flask app
            that returns a JSON dump of the container's environment, so we
            make an HTTP request and parse the response.
            """
            req = get(
                "http://{host}:{port}".format(host=host, port=port),
                persistent=False
            ).addCallback(json_content)
            return req

        d.addCallback(check_result)
        d.addCallback(lambda _: verify_socket(data[u"host"], 8080))
        d.addCallback(lambda _: query_environment(data[u"host"], 8080))
        d.addCallback(
            lambda response:
                self.assertDictContainsSubset(data[u"environment"], response)
        )
        return d


class DatasetAPITestsMixin(object):
    """
    Tests for the dataset API.

    :ivar bytes agent_command: The entry point for the agent to run on each
        node under test.
    """
    agent_command = None

    def _create_test(self):
        """
        Create a dataset on a single-node cluster.

        :return: ``Deferred`` firing with a tuple of (``Cluster``
            instance, dataset dictionary) once the dataset is present in
            actual cluster state.
        """
        # Create a 1 node cluster
        waiting_for_cluster = wait_for_cluster(
            test_case=self, node_count=1, agent_command=self.agent_command
        )

        # Configure a dataset on node1
        def configure_dataset(cluster):
            """
            Send a dataset creation request on node1.
            """
            requested_dataset = {
                u"primary": cluster.nodes[0].address,
                u"dataset_id": unicode(uuid4()),
                u"maximum_size": 67108864,
                u"metadata": {u"name": u"my_volume"}
            }

            return cluster.create_dataset(requested_dataset)
        configuring_dataset = waiting_for_cluster.addCallback(
            configure_dataset
        )

        # Wait for the dataset to be created
        def wait_for_dataset((cluster, dataset)):
            return cluster.wait_for_dataset(dataset)
        waiting_for_create = configuring_dataset.addCallback(wait_for_dataset)

        return waiting_for_create

    def test_dataset_creation(self):
        """
        A dataset can be created on a specific node.
        """
        return self._create_test()


def make_dataset_api_tests(agent_command):
    """
    :param bytes agent_command: The entry point for the agent to run on each
        node.
    :return: An ``IDatasetAPI`` interface ``TestCase``.
    """
    class Tests(DatasetAPITestsMixin, TestCase):
        def setUp(self):
            self.agent_command = agent_command

    return Tests


class GenericLoopbackDatasetAPITests(
        make_dataset_api_tests(b'flocker-dataset-agent')
):
    """
    Acceptance tests for API operations performed by ``flocker-dataset-agent``.
    """


class GenericZFSDatasetAPITests(make_dataset_api_tests(b'flocker-zfs-agent')):
    """
    Acceptance tests for API operations performed by ``flocker-zfs-agent``.
    """


class LegacyZFSDatasetAPITests(make_dataset_api_tests(b'flocker-zfs-agent')):
    """
    Acceptance tests for those API operations which are currently only
    supported by ``flocker-zfs-agent``.
    """
    def test_dataset_move(self):
        """
        A dataset can be moved from one node to another.
        """
        # Create a 2 node cluster
        waiting_for_cluster = wait_for_cluster(
            test_case=self, node_count=2, agent_command=self.agent_command
        )

        # Configure a dataset on node1
        def configure_dataset(cluster):
            """
            Send a dataset creation request on node1.
            """
            requested_dataset = {
                u"primary": cluster.nodes[0].address,
                u"dataset_id": unicode(uuid4()),
                u"metadata": {u"name": u"my_volume"}
            }

            return cluster.create_dataset(requested_dataset)
        configuring_dataset = waiting_for_cluster.addCallback(
            configure_dataset
        )

        # Wait for the dataset to be created
        waiting_for_create = configuring_dataset.addCallback(
            lambda (cluster, dataset): cluster.wait_for_dataset(dataset)
        )

        # Once created, request to move the dataset to node2
        def move_dataset((cluster, dataset)):
            moved_dataset = {
                u'primary': cluster.nodes[1].address
            }
            return cluster.update_dataset(dataset['dataset_id'], moved_dataset)
        dataset_moving = waiting_for_create.addCallback(move_dataset)

        # Wait for the dataset to be moved
        waiting_for_move = dataset_moving.addCallback(
            lambda (cluster, dataset): cluster.wait_for_dataset(dataset)
        )

        return waiting_for_move

    def test_dataset_deletion(self):
        """
        A dataset can be deleted, resulting in its removal from the node.
        """
        created = self._create_test()

        def delete_dataset(result):
            cluster, dataset = result
            deleted = cluster.delete_dataset(dataset["dataset_id"])

            def not_exists():
                request = cluster.datasets_state()
                request.addCallback(
                    lambda actual_datasets: dataset["dataset_id"] not in
                    (d["dataset_id"] for d in actual_datasets))
                return request
            deleted.addCallback(lambda _: loop_until(not_exists))
            return deleted
        created.addCallback(delete_dataset)
        return created

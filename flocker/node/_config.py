# Copyright Hybrid Logic Ltd.  See LICENSE file for details.
# -*- test-case-name: flocker.node.test.test_config -*-

"""
APIs for parsing and validating configuration.
"""

from __future__ import unicode_literals, absolute_import

from ._model import Application, DockerImage, Node, Deployment, Port


class ConfigurationError(Exception):
    """
    Some part of the supplied configuration was wrong.

    The exception message will include some details about what.
    """


class Configuration(object):
    """
    Validate and parse configurations.
    """
    def _applications_from_configuration(self, application_configuration):
        """
        Validate and parse a given application configuration.

        :param dict application_configuration: The intermediate configuration
            representation to load into ``Application`` instances.  See
            :ref:`Configuration` for details.

        :raises ConfigurationError: if there are validation errors.

        :returns: A ``dict`` mapping application names to ``Application``
            instances.
        """
        if u'applications' not in application_configuration:
            raise ConfigurationError("Application configuration has an error. "
                                     "Missing 'applications' key.")

        if u'version' not in application_configuration:
            raise ConfigurationError("Application configuration has an error. "
                                     "Missing 'version' key.")

        if application_configuration[u'version'] != 1:
            raise ConfigurationError("Application configuration has an error. "
                                     "Incorrect version specified.")

        applications = {}
        for application_name, config in (
                application_configuration['applications'].items()):
            try:
                image_name = config.pop('image')
            except KeyError as e:
                raise ConfigurationError(
                    ("Application '{application_name}' has a config error. "
                     "Missing value for '{message}'.").format(
                         application_name=application_name, message=e.message)
                )

            try:
                image = DockerImage.from_string(image_name)
            except ValueError as e:
                raise ConfigurationError(
                    ("Application '{application_name}' has a config error. "
                     "Invalid Docker image name. {message}").format(
                         application_name=application_name, message=e.message)
                )

            ports = []
            try:
                for port in config.pop('ports', []):
                    try:
                        internal_port = port.pop('internal')
                    except KeyError:
                        raise ValueError("Missing internal port.")
                    try:
                        external_port = port.pop('external')
                    except KeyError:
                        raise ValueError("Missing external port.")

                    if port:
                        raise ValueError(
                            "Unrecognised keys: {keys}.".format(
                                keys=', '.join(port.keys())))
                    ports.append(Port(internal_port=internal_port,
                                      external_port=external_port))
            except ValueError as e:
                raise ConfigurationError(
                    ("Application '{application_name}' has a config error. "
                     "Invalid ports specification. {message}").format(
                         application_name=application_name, message=e.message))

            applications[application_name] = Application(
                name=application_name,
                image=image,
                ports=frozenset(ports))

            if config:
                raise ConfigurationError(
                    ("Application '{application_name}' has a config error. "
                     "Unrecognised keys: {keys}.").format(
                         application_name=application_name,
                         keys=', '.join(config.keys()))
                )
        return applications

    def _deployment_from_configuration(self, deployment_configuration,
                                       all_applications):
        """
        Validate and parse a given deployment configuration.

        :param dict deployment_configuration: The intermediate configuration
            representation to load into ``Node`` instances.  See
            :ref:`Configuration` for details.

        :param set all_applications: All applications which should be running
            on all nodes.

        :raises ConfigurationError: if there are validation errors.

        :returns: A ``set`` of ``Node`` instances.
        """
        if 'nodes' not in deployment_configuration:
            raise ConfigurationError("Deployment configuration has an error. "
                                     "Missing 'nodes' key.")

        if u'version' not in deployment_configuration:
            raise ConfigurationError("Deployment configuration has an error. "
                                     "Missing 'version' key.")

        if deployment_configuration[u'version'] != 1:
            raise ConfigurationError("Deployment configuration has an error. "
                                     "Incorrect version specified.")

        nodes = []
        for hostname, application_names in (
                deployment_configuration['nodes'].items()):
            if not isinstance(application_names, list):
                raise ConfigurationError(
                    "Node {node_name} has a config error. "
                    "Wrong value type: {value_type}. "
                    "Should be list.".format(
                        node_name=hostname,
                        value_type=application_names.__class__.__name__)
                )
            node_applications = []
            for name in application_names:
                application = all_applications.get(name)
                if application is None:
                    raise ConfigurationError(
                        "Node {hostname} has a config error. "
                        "Unrecognised application name: "
                        "{application_name}.".format(
                            hostname=hostname, application_name=name)
                    )
                node_applications.append(application)
            node = Node(hostname=hostname,
                        applications=frozenset(node_applications))
            nodes.append(node)
        return set(nodes)

    def model_from_configuration(self, application_configuration,
                                 deployment_configuration):
        """
        Validate and coerce the supplied application configuration and
        deployment configuration dictionaries into a ``Deployment`` instance.

        :param dict application_configuration: Map of applications to Docker
            images.

        :param dict deployment_configuration: Map of node names to application
            names.

        :raises ValueError: if there are validation errors.

        :raises KeyError: if there are validation errors.

        :returns: A ``Deployment`` object.
        """
        applications = self._applications_from_configuration(
            application_configuration)
        nodes = self._deployment_from_configuration(
            deployment_configuration, applications)
        return Deployment(nodes=frozenset(nodes))


model_from_configuration = Configuration().model_from_configuration
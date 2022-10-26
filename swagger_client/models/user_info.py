# coding: utf-8

"""
    Compass - Request Tracker

    API documentation for Compass - Request Tracker. This document contains a complete list of fields for a Compass request accessible to an admin. An authorized user may only read, write, and update certain fields or delete based on their role. All requests require a valid OAuth2 Bearer Token passed as a header that is associated with a Cisco CEC user account.  # noqa: E501

    OpenAPI spec version: 1.0.0
    Contact: compass-devcx@cisco.com
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""

import pprint
import re  # noqa: F401

import six

class UserInfo(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    """
    Attributes:
      swagger_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    swagger_types = {
        'role': 'str',
        'username': 'str',
        'email': 'str',
        'expires_in': 'int',
        'latest_view_id': 'int'
    }

    attribute_map = {
        'role': 'role',
        'username': 'username',
        'email': 'email',
        'expires_in': 'expires_in',
        'latest_view_id': 'latestViewId'
    }

    def __init__(self, role=None, username=None, email=None, expires_in=None, latest_view_id=None):  # noqa: E501
        """UserInfo - a model defined in Swagger"""  # noqa: E501
        self._role = None
        self._username = None
        self._email = None
        self._expires_in = None
        self._latest_view_id = None
        self.discriminator = None
        if role is not None:
            self.role = role
        if username is not None:
            self.username = username
        if email is not None:
            self.email = email
        if expires_in is not None:
            self.expires_in = expires_in
        if latest_view_id is not None:
            self.latest_view_id = latest_view_id

    @property
    def role(self):
        """Gets the role of this UserInfo.  # noqa: E501


        :return: The role of this UserInfo.  # noqa: E501
        :rtype: str
        """
        return self._role

    @role.setter
    def role(self, role):
        """Sets the role of this UserInfo.


        :param role: The role of this UserInfo.  # noqa: E501
        :type: str
        """

        self._role = role

    @property
    def username(self):
        """Gets the username of this UserInfo.  # noqa: E501


        :return: The username of this UserInfo.  # noqa: E501
        :rtype: str
        """
        return self._username

    @username.setter
    def username(self, username):
        """Sets the username of this UserInfo.


        :param username: The username of this UserInfo.  # noqa: E501
        :type: str
        """

        self._username = username

    @property
    def email(self):
        """Gets the email of this UserInfo.  # noqa: E501


        :return: The email of this UserInfo.  # noqa: E501
        :rtype: str
        """
        return self._email

    @email.setter
    def email(self, email):
        """Sets the email of this UserInfo.


        :param email: The email of this UserInfo.  # noqa: E501
        :type: str
        """

        self._email = email

    @property
    def expires_in(self):
        """Gets the expires_in of this UserInfo.  # noqa: E501


        :return: The expires_in of this UserInfo.  # noqa: E501
        :rtype: int
        """
        return self._expires_in

    @expires_in.setter
    def expires_in(self, expires_in):
        """Sets the expires_in of this UserInfo.


        :param expires_in: The expires_in of this UserInfo.  # noqa: E501
        :type: int
        """

        self._expires_in = expires_in

    @property
    def latest_view_id(self):
        """Gets the latest_view_id of this UserInfo.  # noqa: E501


        :return: The latest_view_id of this UserInfo.  # noqa: E501
        :rtype: int
        """
        return self._latest_view_id

    @latest_view_id.setter
    def latest_view_id(self, latest_view_id):
        """Sets the latest_view_id of this UserInfo.


        :param latest_view_id: The latest_view_id of this UserInfo.  # noqa: E501
        :type: int
        """

        self._latest_view_id = latest_view_id

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value
        if issubclass(UserInfo, dict):
            for key, value in self.items():
                result[key] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, UserInfo):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other

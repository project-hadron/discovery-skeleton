import filecmp
import os
import pickle
import shutil
from contextlib import closing
from typing import Any as Canonical

from ds_foundation.handlers.abstract_handlers import AbstractSourceHandler, SourceContract, AbstractPersistHandler

__author__ = 'Darryl Oatridge'


class PythonSourceHandler(AbstractSourceHandler):
    """ A simple pure Python data source handler"""

    def __init__(self, source_contract: SourceContract):
        """ initialise the Hander passing the source_contract dictionary """
        super().__init__(source_contract)
        self._modified = 0

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        # TODO: Fill in the supported data types
        raise NotImplementedError("Not Yet implemented")

    def load_canonical(self) -> Canonical:
        """ returns the canonical dataset based on the source contract
            The canonical in this instance is a dictionary that has the headers as the key and then
            the ordered list of values for that header
        """
        if not isinstance(self.source_contract, SourceContract):
            return {}
        resource = self.source_contract.resource
        source_type = self.source_contract.source_type
        location = self.source_contract.location
        _kwargs = self.source_contract.kwargs
        if _kwargs is None:
            _kwargs = {}

        # TODO: load the data into the Canonical form and set the modified attribute
        raise NotImplementedError("Not Yet implemented")

    def get_modified(self) -> [int, float, str]:
        """ returns if the file has been modified"""
        # TODO: Return the current modified state
        raise NotImplementedError("Not Yet implemented")


class PythonPersistHandler(AbstractPersistHandler):
    """ A simple pure python data persist handler"""

    def __init__(self, source_contract: SourceContract):
        """ initialise the Hander passing the source_contract dictionary """
        super().__init__(source_contract)
        self._modified = 0

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        # TODO: Fill in the supported data types
        raise NotImplementedError("Not Yet implemented")

    def get_modified(self) -> [int, float, str]:
        """ returns if the underlying file has changed"""
        # TODO: Return the current modified state
        raise NotImplementedError("Not Yet implemented")

    def exists(self) -> bool:
        """ Returns True is the canonical exists """
        # TODO: Return the current stae of the canonical
        raise NotImplementedError("Not Yet implemented")

    def load_canonical(self) -> Canonical:
        """ returns the canonical dataset.
            The canonical in this instance is a dictionary that has the headers as the key and then
            the ordered list of values for that header
            """
        if not isinstance(self.source_contract, SourceContract):
            return {}
        resource = self.source_contract.resource
        source_type = self.source_contract.source_type
        location = self.source_contract.location
        _kwargs = self.source_contract.kwargs
        if _kwargs is None:
            _kwargs = {}

        # TODO: load the data into the Canonical form and set the modified attribute
        raise NotImplementedError("Not Yet implemented")

    def persist_canonical(self, canonical: Canonical) -> bool:
        """ persists either the canonical dataset """
        if not isinstance(self.source_contract, SourceContract):
            return False
        resource = self.source_contract.resource
        source_type = self.source_contract.source_type
        location = self.source_contract.location

        # TODO: persist the the Canonical into the handler source
        raise NotImplementedError("Not Yet implemented")

    def remove_canonical(self) -> bool:
        # removes the Canonical from persistance

        # TODO: logic to remove the persisted dataset
        raise NotImplementedError("Not Yet implemented")

    def backup_canonical(self, max_backups=None):
        """ creates a backup of the current source contract resource"""
        if not isinstance(self.source_contract, SourceContract):
            return
        max_backups = max_backups if isinstance(max_backups, int) else 10
        resource = self.source_contract.resource
        location = self.source_contract.location

        # TODO: logic to create a backup copy of the persisted dataset
        raise NotImplementedError("Not Yet implemented")

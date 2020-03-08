from aistac.components.abstract_component import AbstractComponent

from ds_skeleton.intent.skeleton_intent_model import SkeletonIntentModel
from ds_skeleton.managers.skeleton_property_manager import SkeletonPropertyManager

__author__ = ''


class SkeletonComponent(AbstractComponent):

    def __init__(self, property_manager: SkeletonPropertyManager, intent_model: SkeletonIntentModel,
                 default_save=None):
        super().__init__(property_manager=property_manager, intent_model=intent_model, default_save=default_save)

    @classmethod
    def from_uri(cls, task_name: str, uri_pm_path: str, pm_file_type: str = None, pm_module: str = None,
                 pm_handler: str = None, default_save=None, template_source_path: str = None,
                 template_persist_path: str = None, template_source_module: str = None,
                 template_persist_module: str = None, template_source_handler: str = None,
                 template_persist_handler: str = None, **kwargs):
        """ Class Factory Method to instantiates the component application. The Factory Method handles the
        instantiation of the Properties Manager, the Intent Model and the persistence of the uploaded properties.

        by default the handler is local Pandas but also supports remote AWS S3 and Redis. It use these Factory
        instantiations ensure that the schema is s3:// or redis:// and the handler will be automatically redirected

         :param task_name: The reference name that uniquely identifies a task or subset of the property manager
         :param uri_pm_path: A URI that identifies the resource path for the property manager.
         :param pm_file_type: (optional) defines a specific file type for the property manager
         :param default_save: (optional) if the configuration should be persisted. default to 'True'
         :param pm_module: (optional) the module or package name where the handler can be found
         :param pm_handler: (optional) the handler for retrieving the resource
         :param default_save: (optional) if the configuration should be persisted. default to 'True'
         :param template_source_path: (optional) a default source root path for the source canonicals
         :param template_persist_path: (optional) a default source root path for the persisted canonicals
         :param template_source_module: (optional) a default module package path for the source handlers
         :param template_persist_module: (optional) a default module package path for the persist handlers
         :param template_source_handler: (optional) a default read only source handler
         :param template_persist_handler: (optional) a default read write persist handler
         :param kwargs: to pass to the connector contract
         :return: the initialised class instance
         """
        pm_file_type = pm_file_type if isinstance(pm_file_type, str) else 'pickle'
        pm_module = pm_module if isinstance(pm_module, str) else 'aistac.handlers.python_handlers'
        pm_handler = pm_handler if isinstance(pm_handler, str) else 'PythonPersistHandler'
        _pm = SkeletonPropertyManager(task_name=task_name)
        _intent_model = SkeletonIntentModel(property_manager=_pm)
        super()._init_properties(property_manager=_pm, uri_pm_path=uri_pm_path, pm_file_type=pm_file_type,
                                 pm_module=pm_module, pm_handler=pm_handler, **kwargs)
        super()._add_templates(property_manager=_pm, save=default_save,
                               source_path=template_source_path, persist_path=template_persist_path,
                               source_module=template_source_module, persist_module=template_persist_module,
                               source_handler=template_source_handler, persist_handler=template_persist_handler)
        instance = cls(property_manager=_pm, intent_model=_intent_model, default_save=default_save)
        instance.modify_connector_from_template(connector_names=instance.pm.connector_contract_list)
        return instance

    @classmethod
    def _from_remote_s3(cls) -> (str, str):
        """ Class Factory Method that builds the connector handlers an Amazon AWS s3 remote store."""
        _module_name = 'ds_connectors.handlers.aws_s3_handlers'
        _handler = 'AwsS3PersistHandler'
        return _module_name, _handler

    @classmethod
    def _from_remote_redis(cls) -> (str, str):
        """ Class Factory Method that builds the connector handlers an Amazon AWS s3 remote store."""
        _module_name = 'ds_connectors.handlers.redis_handlers'
        _handler = 'RedisPersistHandler'
        return _module_name, _handler

    @property
    def pm(self) -> SkeletonPropertyManager:
        """The properties manager instance"""
        return self._component_pm

    @property
    def intent_model(self) -> SkeletonIntentModel:
        """The intent model instance"""
        return self._intent_model


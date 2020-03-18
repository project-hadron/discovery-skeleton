from aistac.components.abstract_component import AbstractComponent

from ds_skeleton.intent.skeleton_intent_model import SkeletonIntentModel
from ds_skeleton.managers.skeleton_property_manager import SkeletonPropertyManager

__author__ = ''


class SkeletonComponent(AbstractComponent):

    def __init__(self, property_manager: SkeletonPropertyManager, intent_model: SkeletonIntentModel, default_save=None,
                 reset_templates: bool = None, align_connectors: bool = None):
        """ Encapsulation class for the transition set of classes

         :param property_manager: The contract property manager instance for this component
         :param intent_model: the model codebase containing the parameterizable intent
         :param default_save: The default behaviour of persisting the contracts:
                     if False: The connector contracts are kept in memory (useful for restricted file systems)
         :param reset_templates: (optional) reset connector templates from environ variables (see `report_environ()`)
         :param align_connectors: (optional) resets aligned connectors to the template
         """
        super().__init__(property_manager=property_manager, intent_model=intent_model, default_save=default_save,
                         reset_templates=reset_templates, align_connectors=align_connectors)

    @classmethod
    def from_uri(cls, task_name: str, uri_pm_path: str, pm_file_type: str = None, pm_module: str = None,
                 pm_handler: str = None, pm_kwargs: dict = None, default_save=None, reset_templates: bool = None,
                 align_connectors: bool = None, default_save_intent: bool=None, default_intent_level: bool=None,
                 order_next_available: bool=None, default_replace_intent: bool=None):
        """ Class Factory Method to instantiates the components application. The Factory Method handles the
        instantiation of the Properties Manager, the Intent Model and the persistence of the uploaded properties.
        See class inline docs for an example method

         :param task_name: The reference name that uniquely identifies a task or subset of the property manager
         :param uri_pm_path: A URI that identifies the resource path for the property manager.
         :param pm_file_type: (optional) defines a specific file type for the property manager
         :param pm_module: (optional) the module or package name where the handler can be found
         :param pm_handler: (optional) the handler for retrieving the resource
         :param pm_kwargs: (optional) a dictionary of kwargs to pass to the property manager
         :param default_save: (optional) if the configuration should be persisted. default to 'True'
         :param reset_templates: (optional) reset connector templates from environ variables. Default True
                                (see `report_environ()`)
         :param align_connectors: (optional) resets aligned connectors to the template. default Default True
         :param default_save_intent: (optional) The default action for saving intent in the property manager
         :param default_intent_level: (optional) the default level intent should be saved at
         :param order_next_available: (optional) if the default behaviour for the order should be next available order
         :param default_replace_intent: (optional) the default replace existing intent behaviour
         :return: the initialised class instance
         """
        pm_file_type = pm_file_type if isinstance(pm_file_type, str) else 'pickle'
        pm_module = pm_module if isinstance(pm_module, str) else 'aistac.handlers.python_handlers'
        pm_handler = pm_handler if isinstance(pm_handler, str) else 'PythonPersistHandler'
        _pm = SkeletonPropertyManager(task_name=task_name)
        _intent_model = SkeletonIntentModel(property_manager=_pm, default_save_intent=default_save_intent,
                                            default_intent_level=default_intent_level,
                                            order_next_available=order_next_available,
                                            default_replace_intent=default_replace_intent)
        super()._init_properties(property_manager=_pm, uri_pm_path=uri_pm_path, pm_file_type=pm_file_type,
                                 pm_module=pm_module, pm_handler=pm_handler, pm_kwargs=pm_kwargs)
        return cls(property_manager=_pm, intent_model=_intent_model, default_save=default_save,
                   reset_templates=reset_templates, align_connectors=align_connectors)

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


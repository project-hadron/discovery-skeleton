from ds_foundation.handlers.abstract_handlers import ConnectorContract
from ds_foundation.components.abstract_component import AbstractComponent

from ds_skeleton.intent.skeleton_intent_model import SkeletonIntentModel
from ds_skeleton.managers.skeleton_property_manager import SkeletonPropertyManager

__author__ = ''


class SkeletonComponent(AbstractComponent):

    @classmethod
    def from_uri(cls, task_name: str, uri_pm_path: str, default_save=None, **kwargs):
        _pm = SkeletonPropertyManager(task_name=task_name, root_keys=[], knowledge_keys=[])
        _intent_model = SkeletonIntentModel(property_manager=_pm)
        super()._init_properties(property_manager=_pm, uri_pm_path=uri_pm_path, **kwargs)
        return cls(property_manager=_pm, intent_model=_intent_model, default_save=default_save)


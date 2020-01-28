import inspect
import threading
from copy import deepcopy

from ds_foundation.properties.abstract_properties import AbstractPropertyManager
from ds_foundation.intent.abstract_intent import AbstractIntentModel

__author__ = ''


class SkeletonIntentModel(AbstractIntentModel):

    def __init__(self, property_manager: AbstractPropertyManager, default_save_intent: bool=True,
                 intent_next_available: bool=False):
        """initialisation of the Intent class. The 'intent_param_exclude' is used to exclude commonly used method
         parameters from being included in the intent contract, this is particularly useful if passing a canonical, or
         non relevant parameters to an intent method pattern. Any named parameter in the intent_param_exclude list
         will not be included in the recorded intent contract for that method

        :param property_manager: the property manager class that references the intent contract.
        :param default_save_intent: (optional) The default action for saving intent in the property manager
        :param intent_next_available: (optional) if the default level should be set to next available level or zero
        """
        default_save_intent = default_save_intent if isinstance(default_save_intent, bool) else True
        intent_param_exclude = ['inplace', 'canonical']
        super().__init__(property_manager=property_manager, intent_param_exclude=intent_param_exclude,
                         default_save_intent=default_save_intent)
        # globals
        self._default_intent_level = -1 if isinstance(intent_next_available, bool) and intent_next_available else 0

    def intent_method(self, canonical,
                      inplace=False, save_intent: bool=True, level: [int, str]=None):
        """ clean the headers of a pandas DataFrame replacing space with underscore

        :param canonical: the canonical to act this intent on
        :param inplace: if the canonical should be aceted upon in place or a copy returned
        :param save_intent: (optional) if the intent contract should be saved to the property manager
        :param level: (optional) the level of the intent,
                        If None: default's 0 unless the global intent_next_available is true then -1
                        if -1: added to a level above any current instance of the intent section, level 0 if not found
                        if int: added to the level specified, overwriting any that already exist
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy pandas.DataFrame.
        """
        # resolve intent persist options
        if not isinstance(save_intent, bool):
            save_intent = self._default_save_intent
        if save_intent:
            level = level if isinstance(level, (int, str)) and str(level).isdecimal() else self._default_intent_level
            method = inspect.currentframe().f_code.co_name
            self._set_intend_signature(self._intent_builder(method=method, params=locals()), level=level,
                                       save_intent=save_intent)
        # Code block for intent
        if not inplace:
            with threading.Lock():
                canonical = deepcopy(canonical)
        # Intent task

        # return strategy
        if not inplace:
            return canonical
        return

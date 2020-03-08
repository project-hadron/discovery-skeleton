import inspect
import threading
from copy import deepcopy

from aistac.properties.abstract_properties import AbstractPropertyManager
from aistac.intent.abstract_intent import AbstractIntentModel

__author__ = ''


class SkeletonIntentModel(AbstractIntentModel):

    def __init__(self, property_manager: AbstractPropertyManager, default_save_intent: bool = None,
                 intent_next_available: bool = None, default_replace_intent: bool = None):
        """initialisation of the Intent class. The 'intent_param_exclude' is used to exclude commonly used method
         parameters from being included in the intent contract, this is particularly useful if passing a canonical, or
         non relevant parameters to an intent method pattern. Any named parameter in the intent_param_exclude list
         will not be included in the recorded intent contract for that method

        :param property_manager: the property manager class that references the intent contract.
        :param default_save_intent: (optional) The default action for saving intent in the property manager
        :param intent_next_available: (optional) if the default level should be set to next available level or zero
        """
        # set all the defaults
        default_save_intent = default_save_intent if isinstance(default_save_intent, bool) else True
        default_replace_intent = default_replace_intent if isinstance(default_replace_intent, bool) else False
        default_intent_level = -1 if isinstance(intent_next_available, bool) and intent_next_available else 0
        intent_param_exclude = ['inplace', 'canonical']
        intent_type_additions = []
        super().__init__(property_manager=property_manager, intent_param_exclude=intent_param_exclude,
                         default_save_intent=default_save_intent, default_intent_level=default_intent_level,
                         default_replace_intent=default_replace_intent, intent_type_additions=intent_type_additions)

    def run_intent_pipeline(self, canonical, intent_levels: [int, str, list] = None, **kwargs):
        """ Collectively runs all parameterised intent taken from the property manager against the code base as
        defined by the intent_contract.

        It is expected that all intent methods have the 'canonical' as the first parameter of the method signature
        and will contain 'inplace' and 'save_intent' as parameters.

        :param canonical: this is the iterative value all intent are applied to and returned.
        :param intent_levels: (optional) an single or list of levels to run, if list, run in order given
        :param kwargs: additional kwargs to add to the parameterised intent, these will replace any that already exist
        :return Canonical with parameterised intent applied or None if inplace is True
        """
        # test if there is any intent to run
        if self._pm.has_intent():
            # get the list of levels to run
            if isinstance(intent_levels, (int, str, list)):
                intent_levels = self._pm.list_formatter(intent_levels)
            else:
                intent_levels = sorted(self._pm.get_intent().keys())
            for level in intent_levels:
                for method, params in self._pm.get_intent(level=level).items():
                    if method in self.__dir__():
                        params.update(params.pop('kwargs', {}))
                        if isinstance(kwargs, dict):
                            params.update(kwargs)
                        params.update({'inplace': False, 'save_intent': False})
                        canonical = eval(f"self.{method}(canonical, **{params})", globals(), locals())
            return canonical

    def intent_method(self, canonical, inplace: bool=False,
                      save_intent: bool=None, replace_intent: bool=None, intent_level: [int, str]=None):
        """ clean the headers of a pandas DataFrame replacing space with underscore

        :param canonical: the canonical to act this intent on
        :param inplace: if the canonical should be aceted upon in place or a copy returned
        :param replace_intent: if the intent from other levels should be replaced, irrelevant of parameter difference
        :param save_intent: (optional) if the intent contract should be saved to the property manager
        :param intent_level: (optional) the level of the intent,
                        If None: default's 0 unless the global intent_next_available is true then -1
                        if -1: added to a level above any current instance of the intent section, level 0 if not found
                        if int: added to the level specified, overwriting any that already exist
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy pandas.DataFrame.
        """
        # resolve intent persist options
        self._set_intend_signature(self._intent_builder(method=inspect.currentframe().f_code.co_name, params=locals()),
                                   intent_level=intent_level, save_intent=save_intent, replace_intent=replace_intent)
        # intend code block on the canonical

        if not inplace:
            with threading.Lock():
                canonical = deepcopy(canonical)
        # Intent task

        # return strategy
        if not inplace:
            return canonical
        return

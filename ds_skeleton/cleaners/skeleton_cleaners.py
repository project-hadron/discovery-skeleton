import threading
from builtins import staticmethod
from copy import deepcopy
from typing import Any as Canonical

from ds_foundation.cleaners.abstract_cleaners import AbstractCleaners

__author__ = 'Darryl Oatridge'


class SkeletonCleaners(AbstractCleaners):
    """a skeleton implemtation of Cleaner abstract"""

    def __dir__(self):
        rtn_list = []
        for m in dir(SkeletonCleaners):
            if not m.startswith('_'):
                rtn_list.append(m)
        return rtn_list

    @staticmethod
    def run_contract_pipeline(data: Canonical, cleaner_contract: dict, inplace: bool=False) -> dict:
        """ run the contract pipeline

        :param data: the dict to be cleaned
        :param cleaner_contract: the configuration dictionary
        :param inplace: if the passed data should be used or a deep copy
        :return the cleaned dictionary
        .. see filter
        """

        # test there is a config to run
        if not isinstance(cleaner_contract, dict) or cleaner_contract is None:
            if not inplace:
                return data

        # create the copy and use this for all the operations
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # auto clean header
        if cleaner_contract.get('auto_clean_header') is not None:
            settings = cleaner_contract.get('auto_clean_header')
            SkeletonCleaners.auto_clean_header(data, case=settings.get('case'), rename_map=settings.get('rename_map'),
                                               replace_spaces=settings.get('replace_spaces'), inplace=True)
        # auto remove
        if cleaner_contract.get('auto_remove_columns') is not None:
            settings = cleaner_contract.get('auto_remove_columns')
            SkeletonCleaners.auto_remove_columns(data, null_min=settings.get('null_min'),
                                                 nulls_list=settings.get('nulls_list'),
                                                 predominant_max=settings.get('predominant_max'), inplace=True)
        # auto drop duplicates
        if cleaner_contract.get('auto_drop_duplicates') is not None:
            settings = cleaner_contract.get('auto_drop_duplicates')
            SkeletonCleaners.auto_drop_duplicates(data, headers=settings.get('headers'), drop=settings.get('drop'),
                                                  dtype=settings.get('dtype'), exclude=settings.get('exclude'),
                                                  regex=settings.get('regex'),
                                                  re_ignore_case=settings.get('re_ignore_case'), inplace=True)
        # 'to remove'
        if cleaner_contract.get('to_remove') is not None:
            settings = cleaner_contract.get('to_remove')
            SkeletonCleaners.to_remove(data, headers=settings.get('headers'), drop=settings.get('drop'),
                                       dtype=settings.get('dtype'), exclude=settings.get('exclude'),
                                       regex=settings.get('regex'), re_ignore_case=settings.get('re_ignore_case'),
                                       inplace=True)
        # 'to select'
        if cleaner_contract.get('to_select') is not None:
            settings = cleaner_contract.get('to_select')
            SkeletonCleaners.to_select(data, headers=settings.get('headers'), drop=settings.get('drop'),
                                       dtype=settings.get('dtype'), exclude=settings.get('exclude'),
                                       regex=settings.get('regex'), re_ignore_case=settings.get('re_ignore_case'),
                                       inplace=True)
        # 'to bool'
        if cleaner_contract.get('to_bool_type') is not None:
            settings = cleaner_contract.get('to_bool_type')
            SkeletonCleaners.to_bool_type(data, bool_map=settings.get('bool_map'), headers=settings.get('headers'),
                                          drop=settings.get('drop'), dtype=settings.get('dtype'),
                                          exclude=settings.get('exclude'), regex=settings.get('regex'),
                                          re_ignore_case=settings.get('re_ignore_case'), inplace=True)
        # 'to date'
        if cleaner_contract.get('to_date_type') is not None:
            settings = cleaner_contract.get('to_date_type')
            SkeletonCleaners.to_date_type(data, headers=settings.get('headers'), drop=settings.get('drop'),
                                          dtype=settings.get('dtype'), exclude=settings.get('exclude'),
                                          regex=settings.get('regex'), re_ignore_case=settings.get('re_ignore_case'),
                                          as_num=settings.get('as_num'), day_first=settings.get('day_first'),
                                          year_first=settings.get('year_first'), inplace=True)
        # 'to numeric'
        if cleaner_contract.get('to_numeric_type') is not None:
            settings = cleaner_contract.get('to_numeric_type')
            SkeletonCleaners.to_numeric_type(data, headers=settings.get('headers'), drop=settings.get('drop'),
                                             dtype=settings.get('dtype'), exclude=settings.get('exclude'),
                                             regex=settings.get('regex'), re_ignore_case=settings.get('re_ignore_case'),
                                             precision=settings.get('precision'), fillna=settings.get('fillna'),
                                             errors=settings.get('errors'), inplace=True)
        # 'to int'
        if cleaner_contract.get('to_int_type') is not None:
            settings = cleaner_contract.get('to_int_type')
            SkeletonCleaners.to_int_type(data, headers=settings.get('headers'), drop=settings.get('drop'),
                                         dtype=settings.get('dtype'), exclude=settings.get('exclude'),
                                         regex=settings.get('regex'), re_ignore_case=settings.get('re_ignore_case'),
                                         fillna=settings.get('fillna'), errors=settings.get('errors'), inplace=True)
        # 'to float'
        if cleaner_contract.get('to_float_type') is not None:
            settings = cleaner_contract.get('to_float_type')
            SkeletonCleaners.to_float_type(data, headers=settings.get('headers'), drop=settings.get('drop'),
                                           dtype=settings.get('dtype'), exclude=settings.get('exclude'),
                                           regex=settings.get('regex'), re_ignore_case=settings.get('re_ignore_case'),
                                           precision=settings.get('precision'), fillna=settings.get('fillna'),
                                           errors=settings.get('errors'), inplace=True)
        # 'to str'
        if cleaner_contract.get('to_str_type') is not None:
            settings = cleaner_contract.get('to_str_type')
            SkeletonCleaners.to_str_type(data, headers=settings.get('headers'), drop=settings.get('drop'),
                                         dtype=settings.get('dtype'), exclude=settings.get('exclude'),
                                         regex=settings.get('regex'), re_ignore_case=settings.get('re_ignore_case'),
                                         nulls_list=settings.get('nulls_list'), inplace=True)
        if not inplace:
            return data

    @staticmethod
    def auto_clean_header(data: Canonical, case: str=None, rename_map: dict=None, replace_spaces: str=None,
                          inplace: bool=False):
        """ clean the headers of a pandas DataFrame replacing space with underscore

        :param data: the data to drop duplicates from
        :param rename_map: a from: to dictionary of headers to rename
        :param case: changes the headers to lower, upper, title. if none of these then no change
        :param replace_spaces: character to replace spaces with. Default is '_' (underscore)
        :param inplace: if the passed data should be used or a deep copy
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy data.
        """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace:
            return SkeletonCleaners._build_section('auto_clean_header', case=case, rename_map=rename_map,
                                                   replace_spaces=replace_spaces)
        return data

    # drop column that only have 1 value in them
    @staticmethod
    def auto_remove_columns(data: Canonical, null_min: float=None, predominant_max: float=None,
                            nulls_list: [bool, list]=None, auto_contract: bool=True, inplace=False) -> dict:
        """ auto removes columns that are np.NaN, a single value or have a predominat value greater than.

        :param data: the data to auto remove
        :param null_min: the minimum number of null values default to 0.998 (99.8%) nulls
        :param predominant_max: the percentage max a single field predominates default is 0.998
        :param nulls_list: can be boolean or a list:
                    if boolean and True then null_list equals ['NaN', 'nan', 'null', '', 'None']
                    if list then this is considered potential null values.
        :param auto_contract: if the auto_category or to_category should be returned
        :param inplace: if to change the passed data or return a copy (see return)
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy data.
        """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace and auto_contract:
            return SkeletonCleaners._build_section('auto_remove_columns', null_min=null_min,
                                                   predominant_max=predominant_max, nulls_list=nulls_list)
        return data

    @staticmethod
    def auto_drop_duplicates(data: Canonical, headers: [str, list]=None, drop: bool=False, dtype: [str, list]=None,
                             exclude: bool=False, regex: [str, list]=None, re_ignore_case: bool=True,
                             inplace: bool=False) -> [dict, Canonical]:
        """ drops duplicate columns

        :param data: the Canonical data to drop duplicates from
        :param headers: a list of headers to drop or filter on type
        :param drop: to drop or not drop the headers
        :param dtype: the column types to include or excluse. Default None else int, float, bool, object, 'number'
        :param exclude: to exclude or include the dtypes
        :param regex: a regiar expression to seach the headers
        :param re_ignore_case: true if the regex should ignore case. Default is False
        :param inplace: if the passed Canonical, should be used or a deep copy
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy Canonical,.
        """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace:
            return SkeletonCleaners._build_section('auto_drop_duplicates', headers=headers, drop=drop, dtype=dtype,
                                                   exclude=exclude, regex=regex, re_ignore_case=re_ignore_case)
        return data

    @staticmethod
    def to_remove(data: Canonical, headers: [str, list]=None, drop: bool=False, dtype: [str, list]=None,
                  exclude: bool=False, regex: [str, list]=None, re_ignore_case: bool=True,
                  inplace: bool=False) -> [dict, Canonical]:
        """ remove columns from the Canonical,

        :param data: the Canonical data to get the column headers from
        :param headers: a list of headers to drop or filter on type
        :param drop: to drop or not drop the headers
        :param dtype: the column types to include or excluse. Default None else int, float, bool, object, 'number'
        :param exclude: to exclude or include the dtypes
        :param regex: a regiar expression to seach the headers
        :param re_ignore_case: true if the regex should ignore case. Default is False
        :param inplace: if the passed Canonical, should be used or a deep copy
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy Canonical,.
        """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace:
            return SkeletonCleaners._build_section('to_remove', headers=headers, drop=drop, dtype=dtype,
                                                   exclude=exclude,
                                                   regex=regex, re_ignore_case=re_ignore_case)
        return data

    @staticmethod
    def to_select(data: Canonical, headers: [str, list]=None, drop: bool=False, dtype: [str, list]=None,
                  exclude: bool=False, regex: [str, list]=None, re_ignore_case: bool=True,
                  inplace: bool=False) -> [dict, Canonical]:
        """ remove columns from the Canonical,

        :param data: the Canonical data to get the column headers from
        :param headers: a list of headers to drop or filter on type
        :param drop: to drop or not drop the headers
        :param dtype: the column types to include or excluse. Default None else int, float, bool, object, 'number'
        :param exclude: to exclude or include the dtypes
        :param regex: a regiar expression to seach the headers
        :param re_ignore_case: true if the regex should ignore case. Default is False
        :param inplace: if the passed Canonical, should be used or a deep copy
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy Canonical,.
        """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace:
            return SkeletonCleaners._build_section('to_select', headers=headers, drop=drop, dtype=dtype,
                                                   exclude=exclude,
                                                   regex=regex, re_ignore_case=re_ignore_case)
        return data

    @staticmethod
    def to_bool_type(data: Canonical, bool_map, headers: [str, list]=None, drop: bool=False, dtype: [str, list]=None,
                     exclude: bool=False, regex: [str, list]=None, re_ignore_case: bool=True,
                     inplace: bool=False) -> [dict, Canonical]:
        """ converts column to bool based on the map

        :param data: the Canonical data to get the column headers from
        :param bool_map: a mapping of what to make True and False
        :param headers: a list of headers to drop or filter on type
        :param drop: to drop or not drop the headers
        :param dtype: the column types to include or excluse. Default None else int, float, bool, object, 'number'
        :param exclude: to exclude or include the dtypes
        :param regex: a regiar expression to seach the headers
        :param re_ignore_case: true if the regex should ignore case. Default is False
        :param inplace: if the passed Canonical, should be used or a deep copy
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy Canonical,.
        """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace:
            return SkeletonCleaners._build_section('to_bool_type', bool_map=bool_map, headers=headers, drop=drop,
                                                   dtype=dtype, exclude=exclude, regex=regex,
                                                   re_ignore_case=re_ignore_case)
        return data

    @staticmethod
    def to_numeric_type(data: Canonical, headers: [str, list]=None, drop: bool=False, dtype: [str, list]=None,
                        exclude: bool=False, regex: [str, list]=None, re_ignore_case: bool=True, precision: int=None,
                        fillna: str=None, errors: str=None, inplace: bool=False) -> [dict, Canonical]:
        """ converts columns to int type

        :param data: the Canonical data to get the column headers from
        :param headers: a list of headers to drop or filter on type
        :param drop: to drop or not drop the headers
        :param dtype: the column types to include or excluse. Default None else int, float, bool, object, 'number'
        :param exclude: to exclude or include the dtypes
        :param regex: a regiar expression to seach the headers
        :param re_ignore_case: true if the regex should ignore case. Default is False
        :param precision: how many decimal places to set the return values. if None then the number is unchanged
        :param fillna: { num_value, 'mean', 'mode', 'median' }. Default to np.nan
                    - If num_value, then replaces NaN with this number value. Must be a value not a string
                    - If 'mean', then replaces NaN with the mean of the column
                    - If 'mode', then replaces NaN with a mode of the column. random sample if more than 1
                    - If 'median', then replaces NaN with the median of the column
        :param errors : {'ignore', 'raise', 'coerce'}, default 'coerce'
                    - If 'raise', then invalid parsing will raise an exception
                    - If 'coerce', then invalid parsing will be set as NaN
                    - If 'ignore', then invalid parsing will return the input
        :param inplace: if the passed Canonical, should be used or a deep copy
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy Canonical,.
        """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace:
            return SkeletonCleaners._build_section('to_numeric_type', fillna=fillna, errors=errors, headers=headers,
                                                   drop=drop, dtype=dtype, exclude=exclude, regex=regex,
                                                   re_ignore_case=re_ignore_case, precision=precision, )
        return data

    @staticmethod
    def to_int_type(data: Canonical, headers: [str, list]=None, drop: bool=False, dtype: [str, list]=None,
                    exclude: bool=False, regex: [str, list]=None, re_ignore_case: bool=True, fillna: str=None,
                    errors: str=None, inplace: bool=False) -> [dict, Canonical]:
        """ converts columns to int type

        :param data: the Canonical data to get the column headers from
        :param headers: a list of headers to drop or filter on type
        :param drop: to drop or not drop the headers
        :param dtype: the column types to include or excluse. Default None else int, float, bool, object, 'number'
        :param exclude: to exclude or include the dtypes
        :param regex: a regiar expression to seach the headers
        :param re_ignore_case: true if the regex should ignore case. Default is False
        :param fillna: { num_value, 'mean', 'mode', 'median' }. Default to 0
                    - If num_value, then replaces NaN with this number value
                    - If 'mean', then replaces NaN with the mean of the column
                    - If 'mode', then replaces NaN with a mode of the column. random sample if more than 1
                    - If 'median', then replaces NaN with the median of the column
        :param errors : {'ignore', 'raise', 'coerce'}, default 'coerce'
                    - If 'raise', then invalid parsing will raise an exception
                    - If 'coerce', then invalid parsing will be set as NaN
                    - If 'ignore', then invalid parsing will return the input
        :param inplace: if the passed Canonical, should be used or a deep copy
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy Canonical,.
        """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace:
            return SkeletonCleaners._build_section('to_int_type', fillna=fillna, errors=errors, headers=headers,
                                                   drop=drop, dtype=dtype, exclude=exclude, regex=regex,
                                                   re_ignore_case=re_ignore_case)
        return data

    @staticmethod
    def to_float_type(data: Canonical, headers: [str, list]=None, drop: bool=False, dtype: [str, list]=None,
                      exclude: bool=False, regex: [str, list]=None, re_ignore_case: bool=True, precision: int=None,
                      fillna: str=None, errors: str=None, inplace: bool=False) -> [dict, Canonical]:
        """ converts columns to float type

        :param data: the Canonical data to get the column headers from
        :param headers: a list of headers to drop or filter on type
        :param drop: to drop or not drop the headers
        :param dtype: the column types to include or excluse. Default None else int, float, bool, object, 'number'
        :param exclude: to exclude or include the dtypes
        :param regex: a regiar expression to seach the headers
        :param re_ignore_case: true if the regex should ignore case. Default is False
        :param precision: how many decimal places to set the return values. if None then the number is unchanged
        :param fillna: { num_value, 'mean', 'mode', 'median' }. Default to np.nan
                    - If num_value, then replaces NaN with this number value
                    - If 'mean', then replaces NaN with the mean of the column
                    - If 'mode', then replaces NaN with a mode of the column. random sample if more than 1
                    - If 'median', then replaces NaN with the median of the column
        :param errors : {'ignore', 'raise', 'coerce'}, default 'coerce' }. Default to 'coerce'
                    - If 'raise', then invalid parsing will raise an exception
                    - If 'coerce', then invalid parsing will be set as NaN
                    - If 'ignore', then invalid parsing will return the input
        :param inplace: if the passed Canonical, should be used or a deep copy
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy Canonical,.
        """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace:
            return SkeletonCleaners._build_section('to_float_type', fillna=fillna, errors=errors, headers=headers,
                                                   drop=drop, dtype=dtype, exclude=exclude, regex=regex,
                                                   re_ignore_case=re_ignore_case, precision=precision)
        return data

    @staticmethod
    def to_str_type(data: Canonical, headers: [str, list]=None, drop: bool=False, dtype: [str, list]=None,
                    exclude: bool=False, regex: [str, list]=None, re_ignore_case: bool=True, inplace: bool=False,
                    nulls_list: [bool, list]=None) -> [dict, Canonical]:
        """ converts columns to object type

        :param data: the Canonical data to get the column headers from
        :param headers: a list of headers to drop or filter on type
        :param drop: to drop or not drop the headers
        :param dtype: the column types to include or excluse. Default None else int, float, bool, object, 'number'
        :param exclude: to exclude or include the dtypes
        :param regex: a regiar expression to seach the headers
        :param re_ignore_case: true if the regex should ignore case. Default is False
        :param nulls_list: can be boolean or a list:
                    if boolean and True then null_list equals ['NaN', 'nan', 'null', '', 'None']
                    if list then this is considered potential null values.
        :param inplace: if the passed Canonical, should be used or a deep copy
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy Canonical,.
       """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace:
            return SkeletonCleaners._build_section('to_str_type', headers=headers, drop=drop, dtype=dtype,
                                                   exclude=exclude, regex=regex, re_ignore_case=re_ignore_case,
                                                   nulls_list=nulls_list)
        return data

    @staticmethod
    def to_date_type(data: Canonical, headers: [str, list]=None, drop: bool=False, dtype: [str, list]=None,
                     exclude: bool=False, regex: [str, list]=None, re_ignore_case: bool=None, as_num: bool=False,
                     day_first: bool=False, year_first: bool=False, inplace: bool=False) -> [dict, Canonical]:
        """ converts columns to date types

        :param data: the Canonical data to get the column headers from
        :param headers: a list of headers to drop or filter on type
        :param drop: to drop or not drop the headers
        :param dtype: the column types to include or excluse. Default None else int, float, bool, object, 'number'
        :param exclude: to exclude or include the dtypes
        :param regex: a regiar expression to seach the headers
        :param re_ignore_case: true if the regex should ignore case. Default is False
        :param inplace: if the passed Canonical, should be used or a deep copy
        :param as_num: if true returns number of days since 0001-01-01 00:00:00 with fraction being hours/mins/secs
        :param year_first: specifies if to parse with the year first
                If True parses dates with the year first, eg 10/11/12 is parsed as 2010-11-12.
                If both dayfirst and yearfirst are True, yearfirst is preceded (same as dateutil).
        :param day_first: specifies if to parse with the day first
                If True, parses dates with the day first, eg %d-%m-%Y.
                If False default to the a prefered preference, normally %m-%d-%Y (but not strict)
        :return: if inplace, returns a formatted cleaner contract for this method, else a deep copy Canonical,.
        """
        if not inplace:
            with threading.Lock():
                data = deepcopy(data)

        # TODO: Fill in the methods body here

        # returns the contract if inplace
        if inplace:
            return SkeletonCleaners._build_section('to_date_type', headers=headers, drop=drop, dtype=dtype,
                                                   exclude=exclude, regex=regex, re_ignore_case=re_ignore_case,
                                                   as_num=as_num, day_first=day_first, year_first=year_first)
        return data

    @staticmethod
    def _build_section(key, headers=None, drop=None, dtype=None, exclude=None, fillna=None, bool_map=None,
                       null_min=None, null_max=None, single_value=None, nulls_list=None, unique_max=None,
                       regex=None, re_ignore_case=None, case=None, rename_map=None, replace_spaces=None,
                       predominant_max=None, errors=None, precision=None, as_num=None, day_first=None,
                       year_first=None) -> dict:
        """ This private method constrcts the contract sections, returning the section as a dictionary of parameters"""
        section = {}
        if headers is not None:
            section['headers'] = headers
            section['drop'] = drop if drop is not None else False
        if dtype is not None:
            section['dtype'] = dtype
            section['exclude'] = exclude if exclude is not None else False
        if regex is not None:
            section['regex'] = regex
            section['re_ignore_case'] = re_ignore_case if re_ignore_case is not None else False
        if as_num is not None:
            section['as_num'] = as_num
        if day_first is not None:
            section['day_first'] = day_first
        if year_first is not None:
            section['year_first'] = year_first
        if fillna is not None:
            section['fillna'] = fillna
        if errors is not None:
            section['errors'] = errors
        if precision is not None:
            section['precision'] = precision
        if bool_map is not None:
            section['bool_map'] = bool_map
        if null_min is not None:
            section['null_min'] = null_min
        if null_max is not None:
            section['null_max'] = null_max
        if predominant_max is not None:
            section['predominant_max'] = predominant_max
        if nulls_list is not None:
            section['nulls_list'] = nulls_list
        if single_value is not None:
            section['single_value'] = single_value
        if unique_max is not None:
            section['unique_max'] = unique_max
        if case is not None:
            section['case'] = case
        if rename_map is not None:
            section['rename_map'] = rename_map
        if replace_spaces is not None:
            section['replace_spaces'] = replace_spaces
        return {key: section}

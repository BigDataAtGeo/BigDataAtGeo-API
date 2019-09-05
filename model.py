from os.path import join
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DictKey():

    def __init__(self, var: str, scenario: str) -> None:
        self.keys = {
            'var': var,
            'scenario': scenario
        }

    def __repr__(self) -> str:
        return ';'.join(sorted(self.keys.values()))

    def get_filename(self) -> str:
        return f'{self.keys["var"]}_ENS_ufr_remap_{self.keys["scenario"]}_197001-210012_monthly_cont_cbias_year_mean.txt'


class CSVDict(dict):

    def __init__(self, path: str) -> None:
        self.path = path

    def _load_data(self, key: DictKey) -> None:
        data = pd.read_csv(join(self.path, key.get_filename()), skipinitialspace=True)
        self.__dict__[key] = data

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        if key not in self.__dict__:
            logger.info(f'Key not found. Loading {repr(key)}.')
            self._load_data(key)
        return self.__dict__[key]

    def __repr__(self):
        return repr(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __delitem__(self, key):
        del self.__dict__[key]

    def clear(self):
        return self.__dict__.clear()

    def copy(self):
        return self.__dict__.copy()

    def has_key(self, k):
        return k in self.__dict__

    def update(self, *args, **kwargs):
        return self.__dict__.update(*args, **kwargs)

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def pop(self, *args):
        return self.__dict__.pop(*args)

    def __cmp__(self, dict_):
        return self.__cmp__(self.__dict__, dict_)

    def __contains__(self, item):
        return item in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def __unicode__(self):
        return unicode(repr(self.__dict__))

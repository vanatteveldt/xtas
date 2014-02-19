import collections
import datetime

class SAF(collections.OrderedDict):
    """
    Class to represent NLP results in SAF
    https://gist.github.com/vanatteveldt/9027118

    Based on Ordered Dictionary but allows access via attributes
    (e.g. a.tokens instead of a['tokens'])
    All attributes default to lists
    """
    def __getattr__(self, attr):
        if attr.startswith("_"):
            return getattr(super(SAF, self), attr)
        if attr not in self:
            self[attr] = []
        return self[attr]

    def __setattr__(self, attr, value):
        if attr.startswith("_"):
            return super(SAF, self).__setattr__(attr, value)
        self[attr] = value

    def set_header(self, module, version):
        now = datetime.datetime.now().isoformat()
        self.header = {'format': "SAF",
                       'format-version': "0.0",
                       'processed': [{'module': module,
                                      'module-version': version,
                                      "started": now}]}

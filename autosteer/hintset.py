class HintSet:
    """A hint-set describing the disabled knobs; may have dependencies to other hint-sets"""

    def __init__(self, knobs, dependencies):
        self.knobs: set = knobs
        self.dependencies: HintSet = dependencies
        self.plan = None  # store the json query plan
        self.required = False
        self.predicted_runtime = -1.0

    def get_all_knobs(self) -> list:
        """Return all (including the dependent) knobs"""
        return list(self.knobs) + (self.dependencies.get_all_knobs() if self.dependencies is not None else [])

    def __str__(self):
        res = '' if self.dependencies is None else (',' + str(self.dependencies))
        return ','.join(self.knobs) + res
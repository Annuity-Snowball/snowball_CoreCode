import json
import numpy as np
from bson import ObjectId

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, ObjectId):
            # Convert ObjectId to string
            return str(obj)
        else:
            return super(NumpyEncoder, self).default(obj)


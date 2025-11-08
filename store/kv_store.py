class VersionedValue:
    def __init__(self, value, version_vector, dependencies):
        self.value = value
        self.version_vector = version_vector  # dict: {dc_id: counter}
#         self.dependencies = dependencies      # list of version vectors

# class KeyValueStore:
#     def __init__(self):
#         self.store = {}  # key: [VersionedValue]
#     def put(self, key, value, version_vector, dependencies):
#         self.store.setdefault(key, []).append(VersionedValue(value, version_vector, dependencies))
#     def get(self, key):
#         return self.store.get(key, [])
class KeyValueStore:
    def __init__(self):
        self.store = {}
    def put(self, key, value):
        self.store[key] = value
    def get(self, key):
        return self.store.get(key)

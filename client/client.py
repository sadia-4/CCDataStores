class ClientSession:
    def __init__(self, dc):
        self.dc = dc
        self.session_vector = {}
    async def put(self, key, value):
               self.dc.kvstore.put(key, value)

               print(f"Put key={key}, value={value}")
                                                # Include session_vector/dependencies
    
    async def get(self, key):
                
                value = self.dc.kvstore.get(key)

                print(f"Get key={key} -> {value}")
                return value
        # Enforce session guarantees
        

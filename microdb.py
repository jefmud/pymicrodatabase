###############################################
# microdb.py a PICKLED micro-database
# of key-value pairs
#
# version 0.1
###############################################
import json
import pickle, os
import re
import uuid

class MicroDB:
    def __init__(self, filename, preload=True):
        """init constructor sets up the datastore and loads if preload is True"""
        self.datastore = {}
        self.filename = filename
        if preload:
            self._load_data()
            
    def _store_data(self, filename=None):
        """ _store_data(<optional filename>) - private method to 
        save as a pickle list serialization
        Note, you can override the filename if needed
        """
        if filename is None:
            filename = self.filename
        with open(filename, 'wb') as foutput:
            pickle.dump(self.datastore, foutput)
    
    def _load_data(self, filename=None):
        """_load_data( <optional filename> ) - private method load the data, 
        you can override the filename of the class
        """
        if filename is None:
            filename = self.filename
        if os.path.exists(filename):
            # PRIVATE load data from pickle serialized file
            with open(filename, 'rb') as finput:
                self.datastore = pickle.load(finput)
            return self.datastore
        
    def save(self, filename=None):
        """save(filename=None) - save the Database filename optional"""
        self._store_data(filename)
    
    def purge(self):
        """purge - PUBLIC clear/delete persistent storage, but not existing data"""
        if os.path.exists(self.filename):
            os.remove(self.filename)
            return True
        return False
    
    def rename(self, filename, save=True):
        """rename the database"""
        self.filename = filename
        if save:
            self._store_data()
    
    def length(self):
        """length() - returns the length of the datastore"""
        keys = self.datastore.keys()
        return len(keys)
    
    def add(self, key, data, save=True):
        """ADD - add a key/value pair to the store"""
        if not(key in self.datastore):
            self.datastore[key] = data
            if save:
                self._store_data()
            return True
        return False
    
    def addkey(self, data, save=True):
        """
        addkey(data, save=True) - add data and autogenerate key
        returns key if success, False if fails
        """
        key = str(uuid.uuid4())
        if self.add(key, data, save):
            return key
        return False    
            
    def findkey(self, key):
        """findkey(key) - find matching key"""
        if key in self.datastore:
            return self.datastore[key]
        
    def find(self, data=None):
        """find(data) - find matching keys/values"""
        found = []
        for k,v in self.datastore.items():
            if data is None:
                found.append({k:v})
            else:
                if data == k or data == v:
                    found.append({k:v})
                
        return found
    
    def search(self, pattern):
        """find a pattern"""
        found = []
        for key, value in self.datastore.items():
            if isinstance(value, str):
                x = re.search(pattern, value)
                if x:
                    found.append(k)            
        return found
    
    def update(self, key, data, save=True):
        if key in self.datastore:
            self.datastore[key] = data
            if save:
                self._store_data()
            return True
        return False
    
    def delete(self, key, save=True):
        """delete(key) - delete a key from the datastore"""
        if key in self.datastore:
            del self.datastore[key]
            if save:
                self._store_data()
            return True
        return False
    
    def search_subkey(self, key, pattern):
        """search_subkey(key,pattern) - search subkey for regular expression pattern"""
        found = []
        for k, v in self.datastore.items():
            if key in v:
                txt = v[key]
                x = re.search(pattern, txt)
                if x:
                    found.append(k)
        return found
            
    
    def search_subkeys(self, keys, pattern):
        """search_subkeys(keys,pattern) - searh selected subkeys for regular expression pattern"""
        found = []
        for key in keys:
            found += self.search_subkey(key, pattern)
        return found
    
    def to_json(self, filename):
        """
        to_json(filename) - save internal datastore to JSON
        return True if saved to JSON
        raise error is it could not
        """
        with open(filename, 'w') as fd:
            fd.write(json.dumps(self.datastore))
        return True
    
    def load_json(self, filename, save=True):
        """
        load_json(filename) - load internal datastore from JSON
        """
        with open(filename, 'rb') as fd:
            self.datastore = json.loads(fd.read())
        if save:
            self.save()
        return True
            
    def append_json(self, filename, save=True, duplicate=False):
        """append_json(filename) - append JSON"""
        with open(filename, 'rb') as fd:
            datastore = json.loads(fd.read())
            
        for key, data in datastore.items():
            if self.findkey(key):
                dsave = self.add(key, data, save)
            elif duplicate:
                dsave = self.update(key, data, save)
            else:
                dsave = False

            if not dsave:
                print("Error append_json() to datastore on id:{}".format(key))
                
        if save:
            self.save()
        return True
        
if __name__ == '__main__':
    print("Error: Library file")
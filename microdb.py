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
    def __init__(self, filename, preload=True, exist=False):
        """
        MicroDB(filename, preload) - init constructor sets up the datastore
            and loads if preload is True
        
        inputs
            filename - string with location of the file
            preload - boolean (optional True) preloads the datastore from a file,
                False does not preload datastore.
            exist - boolean (optional False) files does not have to exist,
                True if it must prexist
        
        raises
            raises as error if no file
        """
        self.datastore = {}
        self.filename = filename
        if preload:
            self._load_data(filename, exist)
            
    def _store_data(self, filename=None):
        """
        _store_data(<optional filename>) - private method to save as a pickle
                dictionary serialization. Note, you can override the filename if needed
                
        inputs
            filename - stores the data to disk
            
        outputs
            boolean True if success
            
        raises
            file error if cannot save
        """
        if filename is None:
            filename = self.filename
        with open(filename, 'wb') as foutput:
            pickle.dump(self.datastore, foutput)
        return True
    
    def _load_data(self, filename=None, exists=False):
        """
        _load_data( <optional filename> ) - private method load the data, 
               you can override the filename of the class
               
        inputs
            filename - string (optional) where file will be saved,
                if not specified, it will use internal name
            exists - boolean (optional False) does not exist,
                when set to True filename must exist or raise error
        returns
            datastore dictionary
            
        raises
            ValueError if not existing file, or FileError if not a Pickled file
        """
        if filename is None:
            filename = self.filename
        if os.path.exists(filename):
            # PRIVATE load data from pickle serialized file
            with open(filename, 'rb') as finput:
                self.datastore = pickle.load(finput)
            return self.datastore
        
        if exists:
            raise(ValueError, "Filename does not exist")
        
        return self.datastore
        
    def save(self, filename=None):
        """
        save(filename=None) - save the Database filename optional
        
        inputs
            filename - string (optional) where file will be saved,
                if not specified, it will use internal name
                
        raises
            error if file is not written (see _store_data)
        """
        self._store_data(filename)
    
    def purge(self, clear_datastore=False):
        """
        purge() - PUBLIC clear/delete persistent storage, but not existing data
        
        inputs
            clear_datastore - boolean (optional False) DOES NOT clear datastore,
                True if datastore is cleared
            
        outputs
            boolean True if file is removed, False if not
        """
        if os.path.exists(self.filename):
            os.remove(self.filename)
            return True
        return False
    
    def rename(self, filename, save=True):
        """
        rename(filename, save=True) - rename the database
        
        inputs
            filename - the renamed file name
            save - boolean (optional True) saves renamed file to disk, False if not
            
        returns
            None
        """
        self.filename = filename
        if save:
            self._store_data()
    
    def length(self):
        """
        length() - returns the length of the datastore
        
        outputs
            integer - number of keys in the datastore
        """
        keys = self.datastore.keys()
        return len(keys)
    
    def add(self, key, data, save=True):
        """
        add(key, data, save) - add a key/value pair to the store, does not allow duplicates
        
        inputs
            key - key to add
            data - value to store at key
            save - boolean (optional True) saves to disk, False if no save to disk
            
        returns
            boolean - True if saved to datastore, False if key already present.
        """
        if not(key in self.datastore):
            self.datastore[key] = data
            if save:
                self._store_data()
            return True
        return False
    
    def addkey(self, data, save=True):
        """
        addkey(data, save=True) - add data and autogenerate key
        
        inputs
            data - data to save at key
            save - boolean (optional True) saves to disk, False does not
            
        returns
            key - key stored at if success, False if fails
        """
        key = str(uuid.uuid4())
        if self.add(key, data, save):
            return key
        return False    
            
    def findkey(self, key):
        """
        findkey(key) - find matching key
        
        inputs
           (required) key (string or integer)
           
        returns
           None if no matching key
           otherwise returns the value contained in the keu
        """
        if key in self.datastore:
            return self.datastore[key]
        
    def findkeys(self, keys):
        """
        findkeys(keys) - findmatching keys and return data
        
        inputs
            keys - a list of keys in the dictionary
            
        returns
            list of matching items
        """
        data = []
        for key in keys:
            item = self.findkey(key)
            if item:
                data.append(item)
        return data
            
        
    def find(self, data=None):
        """
        find(data) - find matching keys/values
        
        inputs
            data=None returns ALL keyvalue pairs
                 data=any, looks for matches in key and/or values
        returns
            an array of dictionaries
        """
        found = []
        for k,v in self.datastore.items():
            if data is None:
                found.append({k:v})
            else:
                if data == k or data == v:
                    found.append({k:v})
                
        return found
    
    def search(self, pattern):
        """
        search(pattern) - find a pattern
        
        inputs
             a regular expression string
             
        returns
             a list of keys that match
        """
        found = []
        for key, value in self.datastore.items():
            if isinstance(value, str):
                x = re.search(pattern, value)
                if x:
                    found.append(key)            
        return found
    
    def update(self, key, data, save=True):
        """
        update(key, data, save=True)
        
        inputs
            key - dictionary key
            data - value stored at dictionary key
            save - (optional default True) True saves to disk, False does not
        outputs
            True if data was sucessfully entered into datastore
            False if it failed
        """
        if key in self.datastore:
            self.datastore[key] = data
            if save:
                self._store_data()
            return True
        return False
    
    def delete(self, key, save=True):
        """
        delete(key) - delete a key from the datastore
        
        inputs
            key - the key to delete
            save - (optional default True) True saves to disk, False does not
        """
        if key in self.datastore:
            del self.datastore[key]
            if save:
                self._store_data()
            return True
        return False
    
    def search_subkey(self, key, pattern):
        """
        search_subkey(key,pattern) - search subkey for regular expression pattern
        
        inputs
            key - subkey to search for string
            pattern - regular expression to search subkeys
            
        outputs
            list of keys which contain the regular expression
        """
        found = []
        for k, v in self.datastore.items():
            if key in v:
                txt = v[key]
                x = re.search(pattern, txt)
                if x:
                    found.append(k)
        return found
            
    
    def search_subkeys(self, keys, pattern):
        """
        search_subkeys(keys,pattern) - searh selected subkeys for regular expression pattern
        
        inputs
            keys - a list of subkeys to search
            pattern - regular expression to search subkeys
            
        outputs
            list of keys which contain the regular expression
        """
        found = []
        for key in keys:
            found += self.search_subkey(key, pattern)
        return found
    
    def to_json(self, filename):
        """
        to_json(filename) - save internal datastore to JSON
        
        inputs
            filename - filename to save (no existential check)
            
        outputs
           boolean - return True if saved to JSON
           
        raise error is it could not
        """
        with open(filename, 'w') as fd:
            fd.write(json.dumps(self.datastore))
        return True
    
    def load_json(self, filename, save=True):
        """
        load_json(filename) - load internal datastore from JSON
        
        inputs
            filename - filename to save (no existential check)
            save - boolean True indicates it saves to disk, False if not
            
        outputs
            boolean True
            
        raises error if file in NOT JSON
        """
        with open(filename, 'rb') as fd:
            self.datastore = json.loads(fd.read())
        if save:
            self.save()
        return True
            
    def append_json(self, filename, save=True, duplicate=False):
        """
        append_json(filename) - append JSON
        
        inputs
            filename - filename to APPEND from
            save - boolean (optional True) indicates it saves to disk, False if not
            duplicate - boolean (optional True) allow duplicate keys to UPDATE,
                False if no UPDATE of key.
            
        outputs
            boolean - True if success, False if failed
        """
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
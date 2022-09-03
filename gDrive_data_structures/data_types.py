# base element of a google drive object
class gDriveElement:
    def __init__(self, driveObjectData):
        self.id = driveObjectData['id']
        self.name = driveObjectData['name']
        self.mimeType = driveObjectData['mimeType']

    def to_dict(self) -> dict:
        props = copy.deepcopy(vars(self))
        return props

# folder object, inherits the base object
class gFolder(gDriveElement):
    def __init__(self, driveObjectData):
        super().__init__(driveObjectData)
        self.properties = driveObjectData
        self.children = []
        
    def add_child(self, childObject):
        self.children.append(childObject)

class gFile(gDriveElement):
    def __init__(self, driveObjectData):
        super().__init__(driveObjectData)
        self.properties = driveObjectData

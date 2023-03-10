from dynamodb.repository import Repository
from typing import Type, TypeVar
import boto3

T = TypeVar('T')

class DynamoDB(Repository[T]):

    def __init__(self, url: str, table_name: str, cls: Type[T]):
        client = boto3.resource("dynamodb", endpoint_url = url)
        self.cls= cls;
        self.table = client.Table(table_name);
    
    def create_from(self, dictionary):
        obj = self.cls();
        for k, v in dictionary.items():
            obj.__setattr__(k, v)
        
        return obj
    
    def create_from_array(self, array):
        new_array = []
        for item in array:
            new_array.append(self.create_from(item))
        
        return new_array

    def get(self, condition) -> T:
        res = self.table.query(KeyConditionExpression = condition)
        items = res["Items"]
        array = self.create_from_array(items)
        return array
    
    def get_first(self, condition) -> T:
        res = self.table.query(KeyConditionExpression = condition)
        items = res["Items"]

        if (len(items) <= 0):
            return None
        
        model = self.create_from(items[0])
        
        return model
    
    def insert(self, model):
        dictionary = model.__dict__
        response = self.table.put_item(Item = dictionary)
        print(response)        

    def update(self, model):
        pass
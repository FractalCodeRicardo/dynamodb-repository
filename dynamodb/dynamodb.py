import ctypes
from dynamodb.repository import Repository
from typing import Type, TypeVar, List, Dict

import boto3

T = TypeVar('T')

class DynamoDB(Repository[T]):

    def __init__(self, 
                 url: str, 
                 table_name: str, 
                 cls: Type[T]
                 ):
        
        client = boto3.resource("dynamodb", endpoint_url = url)
        self.cls = cls
        self.table = client.Table(table_name);
    
    @staticmethod
    def create_table(url, table_name, key_schema, attributes, capacity):
        client = boto3.resource("dynamodb", endpoint_url = url)
        client.create_table(
            TableName = table_name,
            KeySchema = key_schema,
            AttributeDefinitions = attributes,
            ProvisionedThroughput = capacity
        )
    
    @staticmethod
    def delete_table(url, table_name,):
        client = boto3.resource("dynamodb", endpoint_url = url)
        client.Table(table_name).delete()


    def create_from(self, dictionary: Dict) -> T:
        obj = self.cls();
        for k, v in dictionary.items():
            obj.__setattr__(k, v)
        
        return obj
    

    def create_from_array(self, array) -> List[T]:
        new_array = []
        for item in array:
            new_array.append(self.create_from(item))
        
        return new_array


    def find(self, condition) -> List[T]:
        res = self.table.query(KeyConditionExpression = condition)
        items = res["Items"]
        array = self.create_from_array(items)
        return array
    

    def find_first(self, condition) -> T:
        res = self.table.query(KeyConditionExpression = condition)
        items = res["Items"]

        if (len(items) <= 0):
            return None
        
        model = self.create_from(items[0])
        
        return model
    

    def insert(self, model: T) -> T:
        dictionary = model.__dict__
        self.table.put_item(Item = dictionary)
        return model       


    def columns_without_keys(self, key: Dict, model: Dict) -> List:
        keys = key.keys()
        columns = model.keys()
        filtered_columns = filter(lambda i: not keys.__contains__(i) , columns)

        return list(filtered_columns)


    def get_update_expresion(self, columns: List):
        expressions = map(lambda i: i + "= :" + i, columns)
        expressions = list(expressions)
        return "SET " + ",".join(expressions)


    def get_expression_attributes(self, columns: List, model: Dict):
        values = {}
        for c in columns:
            values[":" + c] = model[c]
        return values


    def update(self, key: Dict, model: Dict) -> T:
        columns = self.columns_without_keys(key, model)
        update_expression = self.get_update_expresion(columns)
        expression_values = self.get_expression_attributes(columns, model)

        resp = self.table.update_item (
            Key = key,
            UpdateExpression = update_expression,
            ExpressionAttributeValues = expression_values,
            ReturnValues = "UPDATED_NEW"
        )
        return model

    def delete(self, key):
        self.table.delete_item(key)

    def any(self, condition):
        res = self.table.query(KeyConditionExpression = condition)
        items = res["Items"]
        return len(items) > 0
import ctypes
from dynamodb.repository import Repository
from boto3.dynamodb.conditions import ConditionBase
from typing import Type, TypeVar, List, Dict

import boto3

T = TypeVar('T')

class DynamoDB(Repository[T]):

    def __init__(self, 
                 url: str, 
                 table_name: str, 
                 cls: Type[T]
                 ):
        
        client = DynamoDB.__get_resource(url);
        self.cls = cls
        self.table = client.Table(table_name);
    

    @staticmethod
    def __get_client(url):
        return boto3.client("dynamodb", endpoint_url = url)
    
    @staticmethod
    def __get_resource(url):
        return boto3.resource("dynamodb", endpoint_url = url)


    @staticmethod
    def create_table(url, table_name, key_schema, attributes, capacity):
        client = DynamoDB.__get_client(url);
        client.create_table(
            TableName = table_name,
            KeySchema = key_schema,
            AttributeDefinitions = attributes,
            ProvisionedThroughput = capacity
        )
    
    @staticmethod
    def delete_table(url, table_name,):
        client = DynamoDB.__get_resource(url);
        client.Table(table_name).delete()


    @staticmethod
    def table_exists(url, table_name):
        client = DynamoDB.__get_client(url);
        response = client.list_tables()
        return table_name in response["TableNames"]


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


    def find(self, condition: ConditionBase) -> List[T]:
        res = self.table.query(KeyConditionExpression = condition)
        items = res["Items"]
        array = self.create_from_array(items)
        return array
    

    def find_first(self, condition: ConditionBase) -> T:
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
        expressions = map(lambda i: "#" + i + "= :" + i, columns)
        expressions = list(expressions)
        return "SET " + ",".join(expressions)


    def get_expression_attributes_values(self, columns: List, model: Dict):
        values = {}
        for c in columns:
            values[":" + c] = model[c]
        return values
    
    def get_expression_attributes_names(self, columns: List):
        values = {}
        for c in columns:
            values["#" + c] = c
        return values


    def update(self, key: Dict, model: T) -> T:
        model_dict = model.__dict__
        columns = self.columns_without_keys(key, model_dict)
        update_expression = self.get_update_expresion(columns)
        expression_values = self.get_expression_attributes_values(columns, model_dict)
        expression_names = self.get_expression_attributes_names(columns)

        self.table.update_item (
            Key = key,
            UpdateExpression = update_expression,
            ExpressionAttributeValues = expression_values,
            ExpressionAttributeNames = expression_names,
            ReturnValues = "UPDATED_NEW"
        )
        return model


    def delete(self, key: Dict):  
        self.table.delete_item(Key = key)


    def any(self, condition: ConditionBase):
        res = self.table.query(KeyConditionExpression = condition)
        items = res["Items"]
        return len(items) > 0
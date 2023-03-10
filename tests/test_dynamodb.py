from unittest import TestCase
from dynamodb.dynamodb import DynamoDB
dynamodb_endpoint = "http://localhost:1111"
dynamodb_table = "Clients"

from boto3.dynamodb.conditions import Key

class Client:
    def __init__(self):
        self.Key = ""
        self.Name = ""


class DynamoDBTest(TestCase):
    
    def get_repo(self):
        repo = DynamoDB(dynamodb_endpoint, dynamodb_table, Client)
        return repo
        
    def test_get(self):
        self.get_repo().get(Key("Key").eq("1"))

    def test_get_first(self):
        condition = Key("Key").eq("1")
        repo = self.get_repo()
        res = repo.get_first(condition)

    def test_insert(self):
        client = Client()
        client.Key = "1"
        client.Name = "Name"
        repo = self.get_repo()
        repo.put(client)

    def test_create_from(self):
        repo = self.get_repo();
        obj = repo.create_from({"Key": "1", "Name": "name"})




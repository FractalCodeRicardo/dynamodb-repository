from boto3.dynamodb.conditions import Key
from unittest import TestCase
import boto3
from dynamodb.dynamodb import DynamoDB
dynamodb_endpoint = "http://localhost:1111"
dynamodb_table = "Clients"


class Client:
    def __init__(self):
        self.Key = ""
        self.Name = ""


class DynamoDBTest(TestCase):

    def test_delete(self):
        DynamoDB.delete_table(dynamodb_endpoint, "no existe")

    def get_repo(self) -> DynamoDB:
        repo = DynamoDB(dynamodb_endpoint, dynamodb_table, Client)
        return repo

    def test_create_from(self):
        dic = {"true": True}
        repo = self.get_repo()
        obj = repo.create_from(dic)
        self.assertEqual(obj.true, True)

    def test_create_from_array(self):
        dic = {"true": True}
        repo = self.get_repo()
        obj = repo.create_from_array([dic])
        self.assertEqual(obj[0].true, True)

    def test_find(self):
        self.get_repo().find(Key("Key").eq("1"))

    def test_get_first(self):
        condition = Key("Key").eq("1")
        repo = self.get_repo()
        res = repo.find_first(condition)

    def test_insert(self):
        client = Client()
        client.Key = "1"
        client.Name = "Name"
        repo = self.get_repo()
        repo.insert(client)

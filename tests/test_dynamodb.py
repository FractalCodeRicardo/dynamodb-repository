from boto3.dynamodb.conditions import Key
from unittest import TestCase
import boto3
from dynamodb.dynamodb import DynamoDB
dynamodb_endpoint = "http://localhost:1111"
dynamodb_table = "Clients"


class Model:
    def __init__(self):
        self.Key = ""
        self.Name = ""


class DynamoDBTest(TestCase):

    def deleteTableIfExists(self):
        if DynamoDB.table_exists(dynamodb_endpoint, dynamodb_table):
            DynamoDB.delete_table(dynamodb_endpoint, dynamodb_table)

    def createTable(self):
        DynamoDB.create_table(
            url=dynamodb_endpoint,
            table_name=dynamodb_table,
            key_schema=[
                {'AttributeName': 'Key', 'KeyType': 'HASH'}
            ],
            attributes=[
                {'AttributeName': 'Key', 'AttributeType': 'S'},
            ],
            capacity={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            })

    def setUp(self) -> None:
        self.deleteTableIfExists()
        self.createTable()

    def get_repo(self) -> DynamoDB:
        repo = DynamoDB(dynamodb_endpoint, dynamodb_table, Model)
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
        repo = self.get_repo()

        model = Model()
        model.Key = "1"

        repo.insert(model)
        res = repo.find(Key("Key").eq("1"))

        self.assertTrue(len(res) > 0)
        self.assertEqual(res[0].Key, "1")

    def test_find_first(self):
        repo = self.get_repo()

        model = Model()
        model.Key = "1"

        repo.insert(model)
        inserted_model = repo.find_first(Key("Key").eq("1"))

        self.assertIsNotNone(inserted_model)
        self.assertEqual(inserted_model.Key, "1")

    def test_insert(self):
        repo = self.get_repo()

        model = Model()
        model.Key = "1"
        model.Name = "Name"
        repo.insert(model)

    def test_columns_without_keys(self):
        keys = {"Key": ""}
        model = Model()
        dict_model = model.__dict__

        repo = self.get_repo()
        columns = repo.columns_without_keys(keys, dict_model)

        self.assertTrue(not "Key" in columns)
        self.assertTrue("Name" in columns)

    def test_get_update_expression(self):
        columns = ["column1", "column2"]
        repo = self.get_repo()
        expression = repo.get_update_expresion(columns)
        self.assertTrue(
            expression == "SET #column1= :column1,#column2= :column2")

    def test_get_expression_attributes(self):
        model = {"Name": "name"}

        columns = ["Name"]
        repo = self.get_repo()
        expression = repo.get_expression_attributes_values(columns, model)
        self.assertTrue(expression[":Name"] == "name")

    def test_update(self):
        model = Model()
        model.Key = "1"
        model.Name = "name"

        repo = self.get_repo()
        repo.insert(model)

        model.Name = "updatedname"
        repo.update(key={"Key": "1"}, model=model)

        model = repo.find_first(Key("Key").eq("1"))

        self.assertIsNotNone(model)
        self.assertEqual(model.Name, "updatedname")

    def test_delete(self):
        model = Model()
        model.Key = "1"

        repo = self.get_repo()
        repo.insert(model)

        repo.delete({"Key": "1"})

        model = repo.find_first(Key("Key").eq("1"))

        self.assertIsNone(model)

    def test_any(self):
        model = Model()
        model.Key = "1"

        repo = self.get_repo()
        repo.insert(model)

        has_elements = repo.any(Key("Key").eq("1"))

        self.assertTrue(has_elements)

from mongoengine import connect, Document, EmbeddedDocument, \
    StringField, IntField, DateTimeField, ListField, EmbeddedDocumentField
from datetime import datetime

connect(db='local',
        username="root",
        password="password123",
        host='localhost',
        port=27017,
        authentication_mechanism='SCRAM-SHA-1',
        authentication_source='admin'
        )


class Employee(EmbeddedDocument):
    """
        社員詳細
    """
    name = StringField(required=True)
    age = IntField(required=False)


SCALE_CHOICES = (
    ("venture", "ベンチャー"),
    ("major", "大手")
)


class Company(Document):
    """
        会社モデル
    """
    name = StringField(required=True, max_length=32)
    scale = StringField(required=True, choices=SCALE_CHOICES)
    created_at = DateTimeField(default=datetime.now())
    members = ListField(EmbeddedDocumentField(Employee))


class TestMongoEngine:
    def add_one(self):
        c_obj = Company(
            name="有名ベンチャー",
            scale="venture",
        )
        c_obj.save()
        return c_obj


if __name__ == "__main__":
    t = TestMongoEngine()
    t.add_one()

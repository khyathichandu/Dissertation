import pymongo
from pymongo import MongoClient
import nltk
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Download the required datasets
nltk.download('stopwords')
nltk.download('punkt')

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Tweet']

# Get the collections
posts = db['Posts']
comments = db['Comments']
users = db['Users']
notifications = db['Notifications']

# Create a new collection for the merged data
merged = db['Merged']

# Create a unique index on user_id to prevent duplicates
merged.create_index("user_id", unique=True)

# Define a function to clean text
def clean_text(text):
    if text is None:
        return ""
    stop_words = set(stopwords.words('english'))
    word_tokens = word_tokenize(text.lower())  # Convert to lowercase
    filtered_text = [w for w in word_tokens if w not in stop_words and w not in string.punctuation]  # Remove punctuation
    return " ".join(filtered_text)

# Define a function to merge specific collections into a user document
def merge_into_user(user_document, collection, field_name):
    related_documents = collection.find({"user_id": user_document["user_id"]})
    user_document[field_name] = [clean_document(doc) for doc in related_documents]

# Define a function to clean specific fields of a document
def clean_document(document):
    if 'body' in document:
        document['body'] = clean_text(document['body'])
    if 'bio' in document:
        document['bio'] = clean_text(document['bio'])
    if 'notification_type' in document:
        document['notification_type'] = clean_text(document['notification_type'])
    document.pop('_id', None)
    return document

# Iterate over the user documents
for user_document in users.find():
    user_document = clean_document(user_document)

    # Merge related documents from other collections into the user document
    merge_into_user(user_document, posts, 'posts')
    merge_into_user(user_document, comments, 'comments')
    merge_into_user(user_document, notifications, 'notifications')

    # Check if the user document already exists in the merged collection
    existing_document = merged.find_one({"user_id": user_document["user_id"]})
    if existing_document:
        # Update the existing document
        merged.update_one({"user_id": user_document["user_id"]}, {"$set": user_document})
    else:
        # Insert the merged user document into the merged collection
        merged.insert_one(user_document)

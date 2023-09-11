import pandas as pd
from pymongo import MongoClient
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.feature_extraction.text import TfidfVectorizer
from textblob import TextBlob

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Tweet']

# Load data from MongoDB into a DataFrame
data = list(db['Merged'].find())
df = pd.DataFrame(data)

# Perform feature selection
selected_features = df[['email_verified', 'has_notifications', 'num_following', 'num_followers',
                        'num_posts', 'post_length', 'month', 'post_content']]

# Impute missing values in 'post_content'
selected_features['post_content'].fillna('missing', inplace=True)

# Extract sentiment score from 'bio' and 'post_content'
selected_features['bio_sentiment'] = df['bio'].apply(lambda x: TextBlob(str(x)).sentiment.polarity)
selected_features['post_sentiment'] = df['post_content'].apply(lambda x: TextBlob(str(x)).sentiment.polarity)

# Vectorize 'post_content' using TF-IDF
tfidf_vectorizer = TfidfVectorizer(max_features=1000)  # Adjust max_features as needed
post_content_tfidf = tfidf_vectorizer.fit_transform(selected_features['post_content'])
post_content_tfidf_df = pd.DataFrame(post_content_tfidf.toarray(),
                                     columns=tfidf_vectorizer.get_feature_names_out())

# Concatenate all selected features
selected_features = pd.concat([selected_features, post_content_tfidf_df], axis=1)

# Split data into train and test sets
X_train, X_test = train_test_split(selected_features, test_size=0.2, random_state=42)

# Scale numeric features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train.drop(columns=['post_content']))
X_test_scaled = scaler.transform(X_test.drop(columns=['post_content']))

# Select top K features using Chi-squared test
num_features_to_select = 20  # Adjust the number of features to select
selector = SelectKBest(score_func=chi2, k=num_features_to_select)
X_train_selected = selector.fit_transform(X_train_scaled, X_train['label'])
X_test_selected = selector.transform(X_test_scaled)

# Create a new DataFrame for the selected features
selected_features_df = pd.DataFrame(X_train_selected, columns=[f'selected_{i}' for i in range(num_features_to_select)])

# Save the selected features to the MongoDB collection "Feature"
selected_features_df['label'] = X_train['label'].values
selected_features_df_dict = selected_features_df.to_dict(orient='records')
db['Feature'].insert_many(selected_features_df_dict)

print("Selected features have been saved to the 'Feature' collection.")

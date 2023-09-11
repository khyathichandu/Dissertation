import pandas as pd
from pymongo import MongoClient
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Tweet']  # Replace 'Tweet' with your actual database name

# Fetch data from 'Merged' collection
merged_collection = db['Merged']  # Replace 'Merged' with your actual collection name
merged_data = list(merged_collection.find())

# Convert merged data to DataFrame
data_flat = pd.DataFrame(merged_data)

# Create new columns using the count function
data_flat['num_posts'] = data_flat['posts'].apply(len)
data_flat['num_followers'] = data_flat['followers_ids'].apply(len)
data_flat['num_following'] = data_flat['following_ids'].apply(len)
data_flat['num_comments'] = data_flat['comments'].apply(len)

# Access the nested 'label' column from the first post
data_flat['label'] = data_flat['posts'].apply(lambda x: x[0]['label'] if len(x) > 0 else None)

# Filter out rows where the 'label' is None
data_flat = data_flat[data_flat['label'].notnull()]

# Extract features and labels
feature_columns = ['num_posts', 'num_followers', 'num_following', 'num_comments', 'has_notifications']
label_column = 'label'
features = data_flat[feature_columns]
labels = data_flat[label_column]

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.2, random_state=42)

# Define a list of classifiers
classifiers = [
    RandomForestClassifier(random_state=42),
    GradientBoostingClassifier(random_state=42),
    SVC(),
    KNeighborsClassifier(),
    LogisticRegression(),
    DecisionTreeClassifier(random_state=42),
    GaussianNB()
]

# Create a Tkinter root window
root = tk.Tk()
root.title("Classifier Evaluation")

# Create a Notebook widget to hold tabs
notebook = ttk.Notebook(root)
notebook.pack(fill=tk.BOTH, expand=True)

# Loop through each classifier and train and evaluate
for clf in classifiers:
    tab = ttk.Frame(notebook)
    notebook.add(tab, text=clf.__class__.__name__)

    fig, axes = plt.subplots(1, 2, figsize=(15, 5))

    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)

    conf_matrix = confusion_matrix(y_test, y_pred)
    sns.heatmap(conf_matrix, annot=True, cmap='Blues', fmt='g', ax=axes[0])
    axes[0].set_title(f'Confusion Matrix - {clf.__class__.__name__}')
    axes[0].set_xlabel('Predicted Label')
    axes[0].set_ylabel('Actual Label')

    class_report = classification_report(y_test, y_pred, zero_division=1)
    print(f"Classifier: {clf.__class__.__name__}")
    print("Classification Report:\n", class_report)
    print("Confusion Matrix:\n", conf_matrix)

    report_lines = class_report.split('\n')[2:-3]
    metrics_data = [line.split()[1:] for line in report_lines]
    metrics_df = pd.DataFrame(metrics_data, columns=['Precision', 'Recall', 'F1-Score', 'Support'])
    metrics_df = metrics_df.astype({'Precision': float, 'Recall': float, 'F1-Score': float, 'Support': float})
    metrics_df.plot(kind='bar', colormap='Paired', ax=axes[1])
    axes[1].set_title(f'Metrics - {clf.__class__.__name__}')
    axes[1].set_xlabel('Metric')
    axes[1].set_ylabel('Score')

    # Display the figure in the Notebook tab
    canvas = FigureCanvasTkAgg(fig, master=tab)
    canvas_widget = canvas.get_tk_widget()
    canvas_widget.pack(fill=tk.BOTH, expand=True)

    # Close the figure after displaying
    plt.close(fig)

# Close the MongoDB connection
client.close()

# Start the Tkinter main loop
root.mainloop()

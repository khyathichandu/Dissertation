import tkinter as tk
from tkinter import ttk
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from wordcloud import WordCloud
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import numpy as np

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Tweet']

# Load data from MongoDB into a DataFrame
data = list(db['Merged'].find())
df = pd.DataFrame(data)

# Extract 'label' and 'post_content' from the nested 'posts' column
df['label'] = df['posts'].apply(lambda x: x[0]['label'] if x else None)
df['post_content'] = df['posts'].apply(lambda x: x[0]['body'] if x else None)

# 1. Basic Overview
def basic_overview():
    print("Shape of the dataset:", df.shape)
    print("\nData types and missing values:\n", df.info())
    print("\nDescriptive statistics for numerical columns:\n", df.describe())
    print("\nValue counts for 'label':\n", df['label'].value_counts())

    # Handle nunique for columns with lists
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            print(f"Number of unique lists in column {col}: {df[col].astype(str).nunique()}")
        else:
            print(f"Number of unique values in column {col}: {df[col].nunique()}")

    print("\nDate range for 'created_at':", df['created_at'].min(), "to", df['created_at'].max())

    # Compute correlation matrix only for numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    print("\nCorrelation matrix:\n", df[numeric_cols].corr())
    
    # Compute skewness only for numeric columns
    print("\nSkewness:\n", df[numeric_cols].skew())
    print("\nKurtosis:\n", df[numeric_cols].kurt())

    # Assuming 'following_ids' and 'followers_ids' are lists of user IDs
    df['num_following'] = df['following_ids'].apply(len)
    df['num_followers'] = df['followers_ids'].apply(len)
    df['num_posts'] = df['posts'].apply(len)

    plt.figure(figsize=(15, 5))

    plt.subplot(1, 3, 1)
    sns.histplot(df['num_following'], bins=30)
    plt.title('Distribution of Number of Users Followed')

    plt.subplot(1, 3, 2)
    sns.histplot(df['num_followers'], bins=30)
    plt.title('Distribution of Number of Followers')

    plt.subplot(1, 3, 3)
    sns.histplot(df['num_posts'], bins=30)
    plt.title('Distribution of Number of Posts per User')



# 2. Target Variable Analysis
def target_variable_analysis():
    if 'label' not in df.columns:
        print("The 'label' column is not present in the dataset.")
        return
    plt.figure()
    sns.countplot(data=df, x='label')
    plt.title("Distribution of Labels (Misleading vs. Non-Misleading)")

# 3. Text Data Analysis
def text_data_analysis():
    if 'post_content' not in df.columns:
        print("The 'post_content' column is not present in the dataset.")
        return
    # Length of posts
    plt.figure()
    df['post_length'] = df['post_content'].apply(lambda x: len(x) if isinstance(x, str) else 0)
    sns.histplot(df['post_length'], bins=50)
    plt.title('Distribution of Post Lengths')

    # Word clouds for posts
    misleading_posts = ' '.join(df[df['label'] == 1]['post_content'].dropna())
    genuine_posts = ' '.join(df[df['label'] == 0]['post_content'].dropna())

    plt.figure(figsize=(10, 7))
    wordcloud_misleading = WordCloud(width=800, height=400, background_color='white').generate(misleading_posts)
    plt.imshow(wordcloud_misleading, interpolation='bilinear')
    plt.axis('off')
    plt.title('Word Cloud for Misleading Posts')

    plt.figure(figsize=(10, 7))
    wordcloud_genuine = WordCloud(width=800, height=400, background_color='white').generate(genuine_posts)
    plt.imshow(wordcloud_genuine, interpolation='bilinear')
    plt.axis('off')
    plt.title('Word Cloud for Genuine Posts')

# 4. Temporal Analysis
def temporal_analysis():
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['month'] = df['created_at'].dt.month
    plt.figure()
    sns.countplot(data=df, x='month')
    plt.title('Posts Distribution by Month')

# 5. User Behavior Analysis
def user_behavior_analysis():
    # Assuming 'following_ids' and 'followers_ids' are lists of user IDs
    df['num_following'] = df['following_ids'].apply(len)
    df['num_followers'] = df['followers_ids'].apply(len)

    plt.figure(figsize=(12, 5))
    plt.subplot(1, 2, 1)
    sns.histplot(df['num_following'], bins=30)
    plt.title('Distribution of Number of Users Followed')

    plt.subplot(1, 2, 2)
    sns.histplot(df['num_followers'], bins=30)
    plt.title('Distribution of Number of Followers')

# 6. Correlation Analysis
def correlation_analysis():
    # Assuming 'num_following' and 'num_followers' are already created in user_behavior_analysis
    correlation_matrix = df[['num_following', 'num_followers']].corr()
    plt.figure()
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
    plt.title('Correlation Analysis')

# 7. Multivariate Analysis
def multivariate_analysis():
    # Example: Scatter plot of 'num_following' vs 'num_followers'
    plt.figure()
    sns.scatterplot(data=df, x='num_following', y='num_followers')
    plt.title('Scatter plot of Number of Users Followed vs Number of Followers')

# 8. Outliers Detection
def outliers_detection():
    # Example: Box plot for 'num_following'
    plt.figure()
    sns.boxplot(df['num_following'])
    plt.title('Box plot for Number of Users Followed')

# 9. Data Quality Issues
def data_quality_issues():
    # Example: Checking for missing values
    missing_data = df.isnull().sum()
    print("Missing data for each column:\n", missing_data)

# 10. Add other EDA functions here...

def plot_to_tab(tab, plot_func):
    """Utility function to plot directly to a tab."""
    plot_func()
    canvas = FigureCanvasTkAgg(plt.gcf(), master=tab)
    canvas_widget = canvas.get_tk_widget()
    canvas_widget.grid(row=0, column=0)
    plt.close()

# Create the main window
root = tk.Tk()
root.title("EDA Results")

# Create the tab control
tabControl = ttk.Notebook(root)

# Create the tabs
tabs = [
    ("Basic Overview", basic_overview),
    ("Target Variable Analysis", target_variable_analysis),
    ("Text Data Analysis", text_data_analysis),
    ("Temporal Analysis", temporal_analysis),
    ("User Behavior Analysis", user_behavior_analysis),
    ("Correlation Analysis", correlation_analysis),
    ("Multivariate Analysis", multivariate_analysis),
    ("Outliers Detection", outliers_detection),
    ("Data Quality Issues", data_quality_issues)
    # ... [Other EDA functions]
]

for tab_name, plot_func in tabs:
    tab = ttk.Frame(tabControl)
    tabControl.add(tab, text=tab_name)
    plot_to_tab(tab, plot_func)

tabControl.pack(expand=1, fill="both")

root.mainloop()

import schedule
import time
from faker import Faker
import random
from datetime import datetime, timedelta
import pandas as pd
import hashlib
import bson
from pymongo import MongoClient
import subprocess


def create_data():
    fake = Faker()

    # Generate synthetic hashtags
    hashtags = ['#' + fake.word() for _ in range(1000)]

    # Generate synthetic mentions
    mentions = ['@' + fake.user_name() for _ in range(1000)]

    # List of promotional keywords
    promo_keywords = ["ad", "sponsored", "promotion", "discount", "sale", "deal", "offer"]

    # Number of records
    n_users = 10000
    n_posts = 10000
    n_comments = 50000
    n_notifications = 2000

    # Users
    user_ids = [str(bson.ObjectId()) for _ in range(n_users)]
    usernames = [fake.user_name() for _ in range(n_users)]
    names = [fake.name() for _ in range(n_users)]
    bios = [fake.text(max_nb_chars=160) for _ in range(n_users)]
    emails = [fake.email() for _ in range(n_users)]
    email_verified = [fake.date_time_between_dates(datetime.now() - timedelta(days=365), datetime.now()) if random.random() > 0.5 else None for _ in range(n_users)]
    images = [fake.image_url() for _ in range(n_users)]
    cover_images = [fake.image_url() for _ in range(n_users)]
    profile_images = [fake.image_url() for _ in range(n_users)]
    hashed_passwords = [hashlib.sha256(fake.password().encode('utf-8')).hexdigest() for _ in range(n_users)]
    created_at_users = [fake.date_time_between_dates(datetime.now() - timedelta(days=365*5), datetime.now()) for _ in range(n_users)]
    updated_at_users = [fake.date_time_between_dates(user_created_at, datetime.now()) for user_created_at in created_at_users]
    following_ids = [[str(bson.ObjectId()) for _ in range(random.randint(0, 50))] for _ in range(n_users)]
    followers_ids = [[str(bson.ObjectId()) for _ in range(random.randint(0, 50))] for _ in range(n_users)]
    has_notifications = [random.choice([True, False]) for _ in range(n_users)]

    # Define misleading users (e.g., 10% of users)
    misleading_users = random.sample(user_ids, int(0.1 * n_users))

    # Posts
    post_ids = [str(bson.ObjectId()) for _ in range(n_posts)]
    post_bodies = [fake.sentence() for _ in range(n_posts)]
    post_user_ids = [random.choice(user_ids) for _ in range(n_posts)]
    post_created_at = [fake.date_time_between_dates(datetime.now() - timedelta(days=365), datetime.now()) for _ in range(n_posts)]
    post_updated_at = [fake.date_time_between_dates(post_created_at, datetime.now()) for post_created_at in post_created_at]
    liked_ids = [[str(bson.ObjectId()) for _ in range(random.randint(0, n_users//10))] for _ in range(n_posts)]
    post_images = [fake.image_url() if random.random() > 0.5 else None for _ in range(n_posts)]
    labels = []

    for i in range(n_posts):
        is_misleading = False

        # If the post is by a misleading user, increase the chance of it being misleading
        if post_user_ids[i] in misleading_users and random.random() > 0.5:
            is_misleading = True

        # Add hashtags
        for _ in range(random.randint(0, 5)):
            post_bodies[i] += ' ' + random.choice(hashtags)
        
        # Add mentions
        for _ in range(random.randint(0, 5)):
            post_bodies[i] += ' ' + random.choice(mentions)
        
        # Add promotional keywords
        if is_misleading:
            for _ in range(random.randint(2, 5)):  # Increase the frequency for misleading posts
                post_bodies[i] += ' ' + random.choice(promo_keywords)
            if random.random() > 0.7:  # 70% chance to not include clear advertising hashtags
                post_bodies[i] = post_bodies[i].replace("#ad", "").replace("#sponsored", "")
            labels.append(1)
        else:
            for _ in range(random.randint(0, 3)):
                post_bodies[i] += ' ' + random.choice(promo_keywords)
            labels.append(0)

    # Comments
    comment_ids = [str(bson.ObjectId()) for _ in range(n_comments)]
    comment_bodies = [fake.sentence() for _ in range(n_comments)]
    comment_user_ids = [random.choice(user_ids) for _ in range(n_comments)]
    comment_post_ids = [random.choice(post_ids) for _ in range(n_comments)]
    comment_created_at = [fake.date_time_between_dates(datetime.now() - timedelta(days=365), datetime.now()) for _ in range(n_comments)]
    comment_updated_at = [fake.date_time_between_dates(comment_created_at, datetime.now()) for comment_created_at in comment_created_at]

    # Notifications
    notification_ids = [str(bson.ObjectId()) for _ in range(n_notifications)]
    notification_bodies = [fake.sentence() for _ in range(n_notifications)]
    notification_user_ids = [random.choice(user_ids) for _ in range(n_notifications)]
    notification_created_at = [fake.date_time_between_dates(datetime.now() - timedelta(days=365), datetime.now()) for _ in range(n_notifications)]

    # Compile the data into DataFrames
    users = pd.DataFrame({
        'user_id': user_ids,
        'name': names,
        'username': usernames,
        'bio': bios,
        'email': emails,
        'email_verified': email_verified,
        'image': images,
        'cover_image': cover_images,
        'profile_image': profile_images,
        'hashed_password': hashed_passwords,
        'created_at': created_at_users,
        'updated_at': updated_at_users,
        'following_ids': following_ids,
        'followers_ids': followers_ids,
        'has_notifications': has_notifications
    })

    posts = pd.DataFrame({
        'post_id': post_ids,
        'body': post_bodies,
        'user_id': post_user_ids,
        'created_at': post_created_at,
        'updated_at': post_updated_at,
        'liked_ids': liked_ids,
        'image': post_images,
        'label': labels
    })

    comments = pd.DataFrame({
        'comment_id': comment_ids,
        'body': comment_bodies,
        'user_id': comment_user_ids,
        'post_id': comment_post_ids,
        'created_at': comment_created_at,
        'updated_at': comment_updated_at
    })

    notifications = pd.DataFrame({
        'notification_id': notification_ids,
        'body': notification_bodies,
        'user_id': notification_user_ids,
        'created_at': notification_created_at
    })

    # Convert datetime columns to string
    users['email_verified'] = users['email_verified'].astype(str)
    users['created_at'] = users['created_at'].astype(str)
    users['updated_at'] = users['updated_at'].astype(str)

    posts['created_at'] = posts['created_at'].astype(str)
    posts['updated_at'] = posts['updated_at'].astype(str)

    comments['created_at'] = comments['created_at'].astype(str)
    comments['updated_at'] = comments['updated_at'].astype(str)

    notifications['created_at'] = notifications['created_at'].astype(str)

    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Tweet']  # use your specific database

    # Load the data directly into MongoDB
    db['Users'].insert_many(users.to_dict('records'))
    db['Posts'].insert_many(posts.to_dict('records'))
    db['Comments'].insert_many(comments.to_dict('records'))
    db['Notifications'].insert_many(notifications.to_dict('records'))

    print("Data creation completed!")

     # Call the merge_and_clean function from your Transform.py script
    print("Calling Transform.py script...")
    result_transform = subprocess.call(["python", r"E:\Sheffield\Dissertation\Chandu\Data\twitter\Tranform.py"])
    if result_transform == 0:
        print("Transform.py script executed successfully!")
        
        # Now, call the EDA.py script
        print("Calling EDA.py script...")
        result_eda = subprocess.call(["python", r"E:\Sheffield\Dissertation\Chandu\Data\twitter\EDA.py"])
        if result_eda == 0:
            print("EDA.py script executed successfully!")
        else:
            print(f"EDA.py script failed with exit code {result_eda}")
    else:
        print(f"Transform.py script failed with exit code {result_transform}")

# Schedule the job every 15 minutes
schedule.every(15).minutes.do(create_data)  

# Call the function directly for testing
create_data()

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)

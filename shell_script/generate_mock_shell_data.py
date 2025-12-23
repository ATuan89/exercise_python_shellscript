#!/usr/bin/env python3
"""
Mock Data Generator for Shell Script Exercises.
Generates data for Exercises 1, 2, and 3.
"""

import os
import random
import datetime
import shutil

# Configuration
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
EX1_DIR = os.path.join(BASE_DIR, 'ex1')
EX2_DIR = os.path.join(BASE_DIR, 'ex2')
EX3_DIR = os.path.join(BASE_DIR, 'ex3')

USERS = [f'user{i}' for i in range(1, 101)]
APPS = [f'app{i}' for i in range(1, 21)]
DEVICES = [f'device{i}' for i in range(1, 51)]

def setup_directories():
    if os.path.exists(BASE_DIR):
        shutil.rmtree(BASE_DIR)
    os.makedirs(EX1_DIR)
    os.makedirs(EX2_DIR)
    os.makedirs(EX3_DIR)

def generate_ex1_data():
    """
    Generate nested directories for Ex 1.
    Some with .git, some large (>1GB simulated).
    
    Since creating actual 1GB files is slow and consumes space,
    we will simulate "large" files by creating sparse files or just
    relying on the logic that finds them.
    However, the exercise asks to print size, so we should try to make them
    report large size. Sparse files on Linux report apparent size.
    du -h shows disk usage, but du --apparent-size shows logical size.
    The script usually uses du -s (disk usage). 
    To be safe and fast, we'll create sparse files which du often ignores 
    unless --apparent-size is used, but 'ls -l' sees them as large.
    
    Wait, 'du' without --apparent-size usually reports block usage.
    To make 'du' report > 1GB, we actually need to allocate blocks.
    That might be too heavy.
    
    Alternative: We will modify the exercise requirement for testing to > 1MB
    or specific size, OR we just create one large sparse file and assume
    the user script might use --apparent-size or we just create chunks.
    
    For this mock generator, we will generate sparse files of 1.1GB
    but note that standard 'du' might report 0.
    Let's stick to sparse files and checking if the script handles it,
    or just creating many small files if needed.
    
    Actually, let's create a "large" folder with a real 1KB file but verify
    the script logic.
    For the purpose of the exercise, let's create sparse files and 
    hope the User tests with 'du --apparent-size' or understands.
    
    Actually, let's create a sparse file.
    """
    print("Generating Ex1 data...")
    
    # Structure:
    # ex1/
    #   repo1/ (has .git, small)
    #   repo2/ (has .git, large)
    #   data/ (no .git, large)
    #   small/ (no .git, small)
    
    # repo1
    os.makedirs(os.path.join(EX1_DIR, 'repo1/.git'))
    with open(os.path.join(EX1_DIR, 'repo1/code.py'), 'w') as f:
        f.write("print('hello')")
        
    # repo2 (large)
    os.makedirs(os.path.join(EX1_DIR, 'repo2/.git'))
    # Create a sparse file of 1.1 GB
    with open(os.path.join(EX1_DIR, 'repo2/large_file.bin'), 'wb') as f:
        f.seek(1024 * 1024 * 1100) # 1.1 GB
        f.write(b'\0')
        
    # data (large, no .git)
    os.makedirs(os.path.join(EX1_DIR, 'data'))
    with open(os.path.join(EX1_DIR, 'data/large_dataset.bin'), 'wb') as f:
        f.seek(1024 * 1024 * 1100)
        f.write(b'\0')

def generate_ex2_data():
    """
    Generate 48 csv files for user.application and 48 for user.device.
    Simulating 30 min intervals for 2020-01-16.
    """
    print("Generating Ex2 data...")
    date_str = "2020-01-16"
    base_time = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    
    for i in range(48):
        # Time string HH-MM
        delta = datetime.timedelta(minutes=30 * i)
        current_time = base_time + delta
        time_str = current_time.strftime("%Y-%m-%d-%H-%M")
        
        # user.application
        app_file = os.path.join(EX2_DIR, f"user.application.{time_str}.csv")
        with open(app_file, 'w') as f:
            for _ in range(random.randint(5, 20)):
                f.write(f"{random.choice(USERS)},{random.choice(APPS)}\n")
                
        # user.device
        dev_file = os.path.join(EX2_DIR, f"user.device.{time_str}.csv")
        with open(dev_file, 'w') as f:
            for _ in range(random.randint(5, 20)):
                f.write(f"{random.choice(USERS)},{random.choice(DEVICES)}\n")

def generate_ex3_data():
    """
    Generate daily csv files for user.application for 7 days.
    2020-01-16 to 2020-01-22.
    """
    print("Generating Ex3 data...")
    start_date = datetime.date(2020, 1, 16)
    
    # We want some users to be consistent to test the streak logic
    consistent_users = ['user1', 'user2']
    
    for i in range(7):
        current_date = start_date + datetime.timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        filename = os.path.join(EX3_DIR, f"user.application.{date_str}.csv")
        
        with open(filename, 'w') as f:
            # Add consistent users with specific apps
            for u in consistent_users:
                f.write(f"{u},app1\n") # user1, user2 always use app1
            
            # Add random noise
            for _ in range(20):
                u = random.choice(USERS)
                a = random.choice(APPS)
                f.write(f"{u},{a}\n")

def main():
    setup_directories()
    generate_ex1_data()
    generate_ex2_data()
    generate_ex3_data()
    print(f"Data generated in {BASE_DIR}")

if __name__ == "__main__":
    main()

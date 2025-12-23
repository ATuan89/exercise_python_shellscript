# Shell Script Exercises

Solutions for the Data Pipelines Shell Script Exercises.

## 📁 Structure

```
shell_script/
├── exercise1.sh          # Directory size checker
├── exercise2.sh          # CSV aggregation
├── exercise3.sh          # Consecutive days tracking
├── generate_sample_data.sh
├── sample_data/          # Test CSV files
└── output/               # Exercise 2 output
```

## 🚀 Quick Start

```bash
cd shell_script/
chmod +x *.sh

# Generate sample data
./generate_sample_data.sh
```

---

## Exercise 1: Directory Size Checker

Finds directories with `.git` using more than 1GB.

```bash
./exercise1.sh /path/to/search
```

**Output:**
```
Scanning for .git directories under: /path/to/search
==============================================

Directory: /path/to/project1
  Size: 1.5G (1610612736 bytes)
  Time: 0.234s

==============================================
Found 1 directories with .git > 1GB
```

---

## Exercise 2: CSV Aggregation

Aggregates 48 CSV files per day (30-min intervals).

```bash
./generate_sample_data.sh 2020-01-16
./exercise2.sh 2020-01-16
```

**Output files:**
- `output/app_users.csv` - application,unique_users
- `output/device_users.csv` - device,unique_users
- `output/app_device_combinations.csv` - all app,device pairs

---

## Exercise 3: Consecutive Days User Tracking

Finds users who used an app every day up to a given date.

```bash
./generate_sample_data.sh
./exercise3.sh 2020-01-18 app1
```

**Output:**
```
Finding users who used 'app1' every day up to 2020-01-18
==============================================
Found 3 daily files up to 2020-01-18

Users who used 'app1' every day (all 3 days):
----------------------------------------------
  user1

==============================================
Found 1 users using 'app1' every day
Completed in 0.012s
```

---

## 📋 Test Commands

```bash
# Exercise 1 - Test with home directory
./exercise1.sh ~

# Exercise 2 - Full test
./generate_sample_data.sh 2020-01-16
./exercise2.sh 2020-01-16
cat output/app_users.csv
cat output/device_users.csv
cat output/app_device_combinations.csv

# Exercise 3 - Full test
./generate_sample_data.sh
./exercise3.sh 2020-01-18 app1
./exercise3.sh 2020-01-20 app2
```

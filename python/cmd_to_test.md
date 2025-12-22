# (1) dimensions user,app
python myscript.py --from_datetime="2025-01-01 00:00:00" --to_datetime="2025-01-08 12:30:00" --user=user2 --granularity=30m --dimensions=user,app

# (2) dimensions user
python myscript.py --from_datetime="2025-01-01 00:00:00" --to_datetime="2025-01-08 12:30:00" --user=user2 --granularity=30m --dimensions=user

# (3) dimensions app
python myscript.py --from_datetime="2025-01-01 00:00:00" --to_datetime="2025-01-08 12:30:00" --user=user2 --granularity=30m --dimensions=app

# (4) multiple users, 1day granularity
python myscript.py --from_datetime="2025-01-05 06:00:00" --to_datetime="2025-01-16 15:00:00" --user=user1,user2 --granularity=1day --dimensions=user

# (5) no user filter, 1day granularity
python myscript.py --from_datetime="2025-01-05 06:00:00" --to_datetime="2025-01-16 15:00:00" --granularity=1day --dimensions=user

# (6) user + app filter
python myscript.py --from_datetime="2025-01-05 06:00:00" --to_datetime="2025-03-16 15:00:00" --user=user1,user2,user99 --app=facebook --granularity=1day --dimensions=user
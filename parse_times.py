import datetime

with open("failure_times", "r") as f:
    lines = f.readlines()
    total = 0
    count = 0
    for i in range(len(lines) - 1):
        t1 = lines[i].split()[1][:12]
        t1 = datetime.datetime.strptime(t1, "%H:%M:%S.%f")
        print t1
        t2 = lines[i + 1].split()[1][:12]
        t2 = datetime.datetime.strptime(t2, "%H:%M:%S.%f")
        print t2
        diff = (t2 - t1).microseconds
        total += diff
        count += 1
    print float(total) / (count * 1000)  # to ms

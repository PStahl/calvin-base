import datetime

with open("dave", "r") as f:
    lines = f.readlines()
    total = 0
    count = 0
    for line in lines:
        t1 = line.split(",")[0][:12]
        print t1[:12]
        t1 = datetime.datetime.strptime(t1, "%M:%S.%f")
        t2 = line.split(",")[1][:12]
        t2 = datetime.datetime.strptime(t2, "%M:%S.%f")
        diff = (t2 - t1).microseconds
        total += diff
        count += 1
    print float(total) / count

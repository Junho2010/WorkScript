import os

base_data = "{},1,0,0,1,30.551491,104.062689,3600,179,180629193858,{}" + "\n"
data = []
for i in range(20000, 20001):
    vehicle_no = "A{}".format(i)
    for m in range(10):
        data.append(base_data.format(vehicle_no, i + m))

with open("A00001.csv", "w") as csvfile:
    csvfile.writelines(data)

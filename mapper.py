import sys,csv,math
centroid_data=[]
data_file=[]

def find_distance(a,b,c,d):
  distance=math.sqrt(math.pow(a-c,2)+math.pow(b-d,2))
  return float(distance)


def read_files():
    dist1=[]
    f = open('/home/node/Downloads/cloud-1/centroids.hdfs', 'r')
    data = f.read()
    for line in data.rstrip().split("\n"):
        cluster_id, centroid_coordinates = line.strip().split("\t")
        centroid_x, centroid_y = centroid_coordinates.strip().split(",")
        g = open('/home/node/Downloads/cloud-1/data.hdfs', 'r')
        data1 = g.read()
        for line1 in data1.rstrip().split("\n"):
            old_cluster_id, data_coordinates = line1.strip().split("\t")
            data_x, data_y = data_coordinates.strip().split(",")
            data_new=[]
            #data_new=xcor.append(ycor)
            dist=find_distance(float(centroid_x),float(centroid_y),float(data_x),float(data_y))
                #print dist
            dist1.append(dist)
            cent0dist = dist1[:5]
            cent1dist = dist1[5:]
            cent0dist.insert(0,0)
            cent1dist.insert(0,1)

    print cent0dist
    print cent1dist
  with open("/home/node/Downloads/data.csv") as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    print cent0dist
    cl_id=[]
    for row in readCSV:
        xcor = float(row[0])
        ycor = float(row[1])
        for i in range(0,5):
            if(cent0dist[i+1]<cent1dist[i+1]):
               cl_id.append(cent0dist[0])
               cl_id.append(xcor)

            else:
                    cl_id.append(cent1dist[0])
                    cl_id.append(xcor)
                    cl_id.append(ycor)

    print cl_id

read_files()





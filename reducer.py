def read_newfiles():
    x=[]
    y=[]
    g = open('/home/node/Downloads/cloud-1/newdata.hdfs', 'r')
    data1 = g.read()
    for line1 in data1.rstrip().split("\n"):
            cluster_id, data_coordinates = line1.strip().split("\t")
            data_x, data_y = data_coordinates.strip().split(",")
            if int(cluster_id)==0:
                x.append(float(data_x))
            elif int(cluster_id)==1:
                y.append(float(data_y))
            print x
            print y


    new_centroid_0=sum(x)/len(x)
    new_centroid_1=sum(y)/len(y)
    #print ('0''\t'+(str(new_centroid_0), str(new_centroid_0)))
    #print ('1''\t'+str(new_centroid_1)+','+str(new_centroid_1))



    with open('/home/node/Downloads/cloud-1/new_centroids.hdfs','w') as f:
        f.write('0'+'\t'+str(new_centroid_0)+','+str(new_centroid_0)+'\n')
        f.write('1'+'\t'+str(new_centroid_1)+','+str(new_centroid_1))








z=read_newfiles()
print z
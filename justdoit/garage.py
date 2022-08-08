from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
from pyspark.sql.functions import *
from pyspark.sql import Window

if __name__ == '__main__':
    spark=SparkSession.builder\
            .appName("PYSPARK 2404")\
            .master("local[*]")\
            .config("spark.driver.bindAddress","localhost")\
            .config("spark.ui.port","4050").\
            getOrCreate()

    df1=spark.read.csv(r"C:\Users\91952\Downloads\customerpy.csv",header=True,inferSchema=True)
    #df1.show()

    df2=spark.read.csv(r"C:\Users\91952\Downloads\vendorpy.csv",header=True,inferSchema=True)
    #df2.show()

    df3=spark.read.csv(r"C:\Users\91952\Downloads\employee11py.csv",header=True,inferSchema=True)
    #df3.show()
    #print(df3.printSchema())

    df4 = spark.read.csv(r"C:\Users\91952\Downloads\sparepartpy.csv", header=True,inferSchema=True)
    df4.show()

    df5 = spark.read.csv(r"C:\Users\91952\Downloads\ser_detpy.csv", header=True,inferSchema=True)
    df5.show()
    #print(df5.printSchema())

    df6 = spark.read.csv(r"C:\Users\91952\Downloads\purchasepy.csv", header=True,inferSchema=True)
    #df6.show()
    #df6.printSchema()

    #ese1 = df3.withColumn('to_date',to_date(df3['EDOJ'],'d-MMM-yy'))
    # ese1.show()





    df1.createOrReplaceTempView("customerpy")
    df2.createOrReplaceTempView("vendorpy")
    df3.createOrReplaceTempView("employee11py")
    df4.createOrReplaceTempView("sparepartpy")
    df5.createOrReplaceTempView("ser_detpy")
    df6.createOrReplaceTempView("purchasepy")


    ####QUESTIONS

    #qwe1)list all the customer service
    # es1=spark.sql("select customerpy.CID,customerpy.CNAME,ser_detpy.TYP_SER from customerpy join ser_detpy using (CID )")
    # es1.show()
    #
    # ese2=df1.join(df5, df1.CID ==df5.CID ).select(df1.CID,df5.TYP_SER,df1.CNAME).show()

    #qwe2)customer who are not serviced
    #ese2=df1.join(df5,df1.CID ==df5.CID,"leftanti").show(truncate=False)

    # ese1=spark.sql("select customerpy.CID,customerpy.CNAME,ser_detpy.TYP_SER from customerpy left join ser_detpy on customerpy.CID =ser_detpy.CID \
    # where ser_detpy.CID is null")
    # ese1.show()

    #QWE3)EMPLOYEE WHO HAVE NOT RECEIVED THE COMMISSION:
    #ese2=df5.join(df3,df5.EID == df3.EID,"right").filter(df5.COMM==0).show()

    #qwe4)name the employee who have max commission
    # ese2 = df7.join(df3,df7.EID==df3.EID,'inner').select(max('comm1').alias("max")).show()
    # ese2.join(df7, df7.comm1 == ese2.max, 'inner').select('*').show()

    # tp1 = ese2.join(df7, df7.comm1 == ese2.max, 'inner').select('*')
    # tp1.join(df3, df3.EID == tp1.EID).select([df3.ENAME, tp1.comm1]).distinct().show()
    # ese2=df5.join(df3,df3.EID == df5.EID,"right").groupby("EID").max(df5.COMM).show()
    #df5.groupBy("EID").max("COMM").show()
    #ese1=spark.sql("select EID,max("COMM") from ser_detpy group by EID").shows

    #ese2= df3.filter(df3.EID.isin(df7.groupBy(col("EID")).max("COMM").collect()[0][0])).show()

    #que5)Show employee name and minimum commission amount received by an employee.
    # df6.show()
    # ese2=df7.join(df3,df3.EID == df7.EID,'inner').select(min('comm1').alias("abc")).show()
    # #ese2.join(df7,df7.comm1==ese2.abc,'inner').select('*')
    # tp1= ese2.join(df7,df7.comm1==ese2.abc,'inner').select('*')
    # tp1.join(df3,df3.EID==tp1.EID).select([df3.ENAME,tp1.comm1]).distinct().show()

    #ese1=spark.sql("select distinct employee11py.ENAME,ser.comm1 from employee11py join ser using (EID) where ser.comm1=(select max(comm1) from ser)").show()




    #que6)Display the Middle record from any table.
    # ese1=spark.sql("select * from (select C.* ,ROWNUM RNM from customerpy C) where RNM=(select round (count(*)/2)from customerpy").show()

    #ese1=spark.sql("select * from employee11py where ROWNUM <= (select (count(*)/2) from employee11py").show()

    #quw7)Display last 4 records of any table.
    #spark.sql("select * from (rank() over (order by CID desc) as rank from customerpy c)x where x.rank < 4  ").show()
    # df3.orderBy(col('EID').desc()).show(4)
    # df3.tail(4)
    # df3.take(4)
    # df3.limit(4)
    # ese1=spark.sql(("select * from customerpy order by CID desc")).show(4)


    #que8)Count the number of records without count function from any table.
    # x=df3.select(count('EID')).show()
    # df3.select(length('EID')).show()
    #
    # ese1=spark.sql("select count('EID') FROM employee11py").show()



    #que9)Delete duplicate records from "Ser_det" table on cid.(note Please rollback after execution)


    #10)Show the name of Customer who have paid maximum amount
    #ese2=df1.join(df5,df1.CID==df5.CID,'inner').select(max(col('SP_AMT'))).show()

    # ESE1=spark.sql("select customerpy.CNAME,ser_detpy.SP_AMT from customerpy join ser_detpy where ser_detpy.SP_AMT=(select max(SP_AMT) from ser_detpy)" ).show()

    #Q.11 Display Employees who are not currently working.
    #ese2=df3.select(['EDOL','ENAME']).filter("EDOL is null").show()
    #ese1=spark.sql("select ENAME,EDOL FROM employee11py where EDOL is null").show()

    # Q.12 How many customers serviced their two wheelers.
    # ese1=spark.sql("select count(*) from (select TYP_VEH,TYP_SER,count(CID) FROM ser_detpy  WHERE TYP_VEH = 'TWO WHEELER' group by TYP_VEH,TYP_SER )")
    # ese1.show()

    # ese1 = spark.sql("select count(*) from (select TYP_VEH,count(CID) FROM ser_detpy  group by TYP_VEH,CID having TYP_VEH = 'TWO WHEELER' )")
    # ese1.show()
# ese2=df5.select('CID','TYP_VEH').filter(col("TYP_VEH") == 'TWO WHEELER').groupby(col("CID"),col("TYP_VEH")).count().count()
# print(ese2)



# Q.13 List the Purchased Items which are used for Customer Service with Unit of that Item.

# Q.14 Customers who have Colored their vehicles.
# ese1=spark.sql("select TYP_SER,CID from ser_detpy where TYP_SER= 'COLOR'").show()
# ese2=df5.select('TYP_SER','CID').filter(col("TYP_SER") == 'COLOR').show()


# Q.15 Find the annual income of each employee inclusive of Commission
 #Q.16 Vendor Names who provides the engine oil.
# ese2=df6.join(df2,df6.VID==df2.VID,'INNER').select(df2.VNAME,df6.VID,df6.SPID)
# ese4=ese2.join(df4,ese2.SPID=df4.SPID,'inner').f

# ese1=spark.sql("select purchasepy.VID,vendorpy.VNAME,purchasepy.SPID from purchasepy join vendorpy")
# ese5=spark.sql("select sparepart.SPNAME,ese1.SPID from ese1 join sparepart")

# Q.17 Total Cost to purchase the Color and name the color purchased.
# ese2=df4.select('SPNAME','SPRATE').where(col('SPNAME').like("%COLOUR")).show()
# ese1=spark.sql("select SPNAME,SPRATE from sparepartpy where SPNAME like '%COLOUR'").show()


# Q.18 Purchased Items which are not used in "Ser_det".

#ese2=df6.join(df5,df6.SPID==df5.SPID,'leftanti').show()
# Q.19 Spare Parts Not Purchased but existing in Sparepart
##same logic
# Q.20 Calculate the Profit/Loss of the Firm. Consider one month salary of each employee for Calculation.

#Q.21 Specify the names of customers who have serviced their vehicles more than one time.
# ese2=df5.select('CID','TYP_SER').groupby(col("CID"),col("TYP_SER")).count()
# df8=ese2.filter("count('CID') > 1").show()
# ese1=spark.sql("select TYP_SER,CID,count(CID) from ser_detpy group by TYP_SER,CID having count(CID) > 1").show()

# Q.22 List the Items purchased from vendors locationwise.
# ese1=spark.sql('''select VADD ,count(*) from (select purchasepy.PID,vendorpy.VADD,count(vendorpy.VADD)
# from purchasepy join vendorpy using(VID) group by vendorpy.VADD,purchasepy.PID) group by (VADD)''')
# ese1.show()

# ese2=df2.join(df6,df2.VID == df6.VID,'inner').groupby(df2.VADD).count()
# ese2.show()



# Q.23 Display count of two wheeler and four wheeler from ser_details

# ese1=spark.sql("select TYP_VEH,count(*) from ser_detpy group by TYP_VEH").show()

# ese2=df5.groupby('TYP_VEH').count()
# ese2.show()

# Q24 Display name of customers who paid highest SPGST and for which item

# ese2=df1.join(df5,df5.CID==df1.CID,'full')
# ese3=ese2.join(df6,ese2.SPID==df6.SPID,'full').select(df6.SPGST,ese2.CNAME).orderBy(asc_nulls_last(df6.SPGST)).show(1)

#ese1=spark.sql('''select customerpy.CID,purchasepy.SPGST,customerpy.CNAME from ser_detpy full join customerpy on (ser_detpy.CID = customerpy.CID)
            #full join purchasepy on (purchasepy.SPID = ser_detpy.SPID ) where SPGST= (select max(SPGST) from purchasepy)''').show()

# Q25 Display vendors name who have charged highest SPGST rate  for which item


# Q26list name of item and employee name who have received item
# ese1=spark.sql('''select * from purchasepy full join sparepartpy using (SPID)
#             full join employee11py on purchasepy.RCV_EID == employee11py.EID''').show()

# ese2=df3.join(df6,df3.EID==df6.RCV_EID).select(df6.RCV_EID,df6.SPID,df3.EID,df3.ENAME)
# ese3=ese2.join(df4,ese2.SPID==df4.SPID).select(df4.SPNAME,ese2.ENAME).show()

#Q27 Display the Name and Vehicle Number of Customer who serviced his vehicle, And Name the Item used for Service, And specify the purchase ded

# date of that Item with his vendor and Item Unit and Location, And employee Name who serviced the vehicle. for Vehicle NUMBER "MH-14PA335".'
#Q28 who belong this vehicle  MH-14PA335" Display the customer name
#ese1=df5.join(df1,df5.CID==df1.CID).select(df1.CNAME,df5.VEH_NO,df5.TYP_VEH).where(col('VEH_NO')== 'MH-14PA335').show()
#ese2=spark.sql("select customerpy.CNAME,ser_detpy.TYP_VEH,ser_detpy.VEH_NO from ser_detpy join customerpy ").show()

# ese2=spark.sql("select VEH_NO, CID ,TYP_VEH from (select * FROM ser_detpy  )")
# ese3=spark.sql("select CNAME,CID from customerpy ")
# ese4=spark.sql("select VEH_NO,CID from ese3 join ese2 using (CID) where VEH_NO = 'MH-14PA335' ")

# spark.sql('''select c.CNAME,s.CID,s.VEH_NO,s.TYP_VEH from customerpy c inner join ser_detpy s
#                 on (s.CID = c.CID) where s.VEH_NO = "MH-14PA335"''').show()
#ese4=spark.sql("select ese2.VEH_NO,ese2.TYP_VEH,ese3.CNAME from ese2 join ese3 using(CID)").show()

# Q29 Display the name of customer who belongs to New York and when he /she service their  vehicle on which date
# spark.sql("select c.CID,c.CADD,s.CID,s.SER_DATE from customerpy c inner join ser_detpy s  on c.CID == s.CID where c.CADD = 'NEW YORK' ").show()
# ese2=df1.join(df5,df1.CID==df5.CID,'inner').select(df1.CID,df1.CADD,df5.CID,df5.TYP_SER,df5.SER_DATE).where(df1.CADD == 'NEW YORK').show()

# Q 30 from whom we have purchased items having maximum cost?

# ese1=spark.sql("select v.VNAME, v.VID,p.VID,p.TOTAL from purchasepy p join vendorpy v on v.VID == p.VID  group by p.VID,p.TOTAL,v.VNAME,v.VID ").show()
# ese2=df2.join(df6, df2.VID == df6.VID).select(df2.VNAME,df6.TOTAL).select(max(df6.TOTAL)).show()
# ese3=spark.sql("select P.VID,P.TOTAL,v.VNAME,v.VID from purchasepy P join vendorpy v  on P.VID == v.VID where P.TOTAL =(select MAX(TOTAL) FROM purchasepy)").show()
#
# # Q31 Display the names of employees who are not working as Mechanic and that employee done services
# ese2=spark.sql("select e.ENAME,e.EJOB from employee11py e join ser_detpy s on e.EID == s.EID where e.EJOB != 'MECHANIC' ").show()
# ese1=df3.join(df5,df3.EID==df5.EID).select(df3.ENAME,df3.EJOB).filter(df3.EJOB != 'MECHANIC').show()

# Q32 Display the various jobs along with total number of employees in each job. The output should
# contain only those jobs with more than two employees.
# ese2=spark.sql("select EJOB,count(*) from employee11py group by EJOB having count(1) > 2").show()
# ese1=df3.select('ENAME','EJOB').groupby('EJOB').count()
# ese3=ese1.filter(col('count') > 2).show()

# Q33 Display the details of employees who done service  and give them rank according to their no. of services .
#ese2=spark.sql("select e.EID,e.ENAME,e.EJOB,s.TYP_SER,count(s.TYP_SER) as row_num_desc from employee11py e,ser_detpy s  ").show()
# ese1=df5.withColumn("rank",rank().over(Window.orderBy('TYP_SER'))).show()
# ese3 = df3.join(df5,df3.EID == df5.EID).select(df3.EID,df3.ENAME,df3.EJOB,df5.TYP_SER)\
#     .withColumn("rank",dense_rank().over(Window.orderBy('TYP_SER')))
# ese3.show()

# Q 34 Display those employees who are working as Painter and fitter and who provide service and total count of service done by fitter and painter
#ese1=df3.join(df5,df3.EID == df5.EID).select(df3.ENAME,df3.EJOB,df5.TYP_SER).filter(df3.EJOB == 'FITTER' | df3.EJOB == 'PAINTER').show()
#ese2=spark.sql("select e.ENAME,e.EJOB,s.TYP_SER,count(*) from employee11py e join ser_detpy s on e.EID == s.EID group by e.ENAME,e.EJOB,s.TYP_SER having e.EJOB = 'FITTER' or e.EJOB ='PAINTER' ").show()

# Q35 Display employee salary and as per highest  salary provide Grade to employee
# ese1=df3.withColumn("grade", when(df3.ESAL <= 1200,"C")
#                                  .when(df3.ESAL <= 1800,"B")
#                                  .when(df3.ESAL <= 2500,"A")
#                                  .when(df3.ESAL.isNull() ,"")
#                                  .otherwise(df3.ESAL))
# ese1.show()

# ese2=spark.sql('''select ESAL, case
#                     when ESAL <= 1200 then "C"
#                     when ESAL <= 1800 then "B"
#                     WHEN ESAL <= 2300 THEN "A"
#                     ELSE " "
#                     END AS GRADE
#                     FROM EMPLOYEE11PY ''')
# ese2.show()

# Q36  display the 4th record of emp table without using group by and rowid
#ese1=df3.withColumn("4th",row_number().over(Window.orderBy('EID'))).where(col('4th') == 4).show()
#ese2=spark.sql('''select *,row_number() over(order by EID ) from employee11py ''').show()

# Q37 Provide a commission 100 to employees who are not earning any commission.
# ese1=df3.join(df5,df3.EID == df5.EID)
# ese2=ese1.select(df3.ENAME,df5.COMM).withColumn("COMM1",when(ese1.COMM == 0,"100")
#                  .otherwise(ese1.COMM))
# ese2.show()

# ese2=spark.sql('''select e.ESAL,e.ENAME,s.COMM, case
#     when s.COMM == 0 THEN 100
#     ELSE s.COMM
#     END AS COMM1
#     from employee11py e join ser_detpy s on e.EID== s.EID ''').show()

# ese2=spark.sql('''select e.ESAL,e.ENAME,s.COMM,decode
#                (s.COMM,0,100)COMM1 from employee11py e join ser_detpy s on e.EID== s.EID ''').show()

# select e.eid,e.ename,sd.comm,decode(sd.comm,0,100)new_comm from employee_tab e
# join ser_det sd on e.eid=sd.eid



# Q38 write a query that totals no. of services  for each day and place the results # in descending order

# ese1=df5.orderBy('SER_DATE').groupby("SER_DATE").count()
# ese1.show()
#
# ese2=spark.sql("select SER_DATE,TYP_SER ,count(*) from ser_detpy group by SER_DATE,TYP_SER").show()

#Q39 Display the service details of those customer who belong from same city
# ese2=df5.join(df1,df5.CID==df1.CID).select(df1.CADD,df5.TYP_SER).orderBy(df1.CADD).show()
# ese1=spark.sql("select c.CADD,s.TYP_SER from customerpy c join ser_detpy s on c.CID== s.CID order by s.TYP_SER").show()


# Q40 write a query join customers table to itself to find all pairs of
# customers service by a single employee
#q.41`List each service number follow by name of the customer who
#made  that service
#ese1=df1.join(df5,df1.CID == df5.CID,'INNER').select(df1.CNAME,df5.SID).show()
# Q42 Write a query to get details of employee and provide rating on basis of  maximum services provide by employee  .Note (rating should be like A,B,C,D)
#ese1=df5.select('SID','EID').groupby('SID').max('SID').show()

# Q43 Write a query to get maximum service amount of each customer with their customer details ?
#df1.join(df5,df1.CID == df5.CID,'INNER').select(df1.CNAME,df1.CADD,df5.SER_AMT).distinct().show()
# Q44 Get the details of customers with his total no of services ?
# ese1=df1.join(df5,df1.CID == df5.CID,'INNER').select(df1.CNAME,df1.CID,df5.SID).groupby('CID').count()
# ese1.show()

# Q45 From which location sparpart purchased  with highest cost ?
#ese2=df2.join(df6,df2.VID==df6.VID,'inner').select(df2.VADD,df6.SPRATE).groupby('VADD').max('SPRATE').show()

# Q46 Get the details of employee with their service details who has salary is null
#ese2=df3.join(df5,df3.EID==df5.EID,'full').select('ENAME','ESAL','TYP_VEH','TYP_SER').where(df3.ESAL == "null").show()

#ese1=spark.sql("select e.ESAL,e.ENAME,s.TYP_VEH,s.TYP_SER FROM employee11py e full join ser_detpy s on e.EID == s.EID where ESAL is null").show()

# Q47 find the sum of purchase location wise
# df2.join(df6,df2.VID == df6.VID,'inner').select(df2.VADD,df6.TOTAL).groupby('VADD').sum('TOTAL').show()
# spark.sql("select v.VADD,sum(p.TOTAL) from vendorpy v join purchasepy p on v.VID==p.VID group by VADD ").show()


# Q48 write a query sum of purchase amount in word location wise ?



# Q49 Has the customer who has spent the largest amount money has
# been give highest rating
# ese1=df5.withColumn("rating", when(df5.TOTAL <= 500,"C")
#                                  .when(df5.TOTAL <= 1000,"B")
#                                  .when(df5.TOTAL <= 1500,"A")
#                                  .otherwise(df5.TOTAL))
#
# ese1.groupby('CID','rating').max('TOTAL').show()


# Q50 select the total amount in service for each customer for which
# the total is greater than the amount of the largest service amount in the table


#ese3=df5.select(df5.TOTAL,df5.SER_AMT).where( df5.TOTAL > (df5.select('SER_AMT').max('SER_AMT'))).show()
#ese1=spark.sql("select TOTAL,SER_AMT from ser_detpy  where TOTAL > (SELECT MAX(SER_AMT) from ser_detpy)").show()

# Q51  List the customer name and sparepart name used for their vehicle and  vehicle type

# ese1=df1.join(df5,df1.CID == df5.CID,'full')
# ese3=ese1.join(df4,ese1.SPID == df4.SPID).select(ese1.CNAME,df4.SPNAME,ese1.TYP_VEH).show()

# Q52 Write a query to get spname ,ename,cname quantity ,rate ,service amount for record exist in service table
# ese1=df1.join(df5,df1.CID == df5.CID,'full')
# ese3=ese1.join(df3,df3.EID == ese1.EID).select('CNAME','ENAME','SP_RATE','SER_AMT').show()

# Q53 specify the vehicles owners who’s tube damaged.

#ese1=df1.join(df5,df1.CID == df5.CID,'inner').select('CNAME','VEH_NO','TYP_SER').where(col('TYP_SER') == 'TUBE DAMAGED').show()

# Q.54 Specify the details who have taken full service.
#ese1=df5.filter(col('TYP_SER') == 'FULL SERVICING').show()

# Q.55 Select the employees who have not worked yet and left the job.
# df3.join(df5,df3.EID == df5.EID,'full').select('ENAME','EDOL',df3.EID,df5.EID)\
#     .filter(col('EDOL') != 'null' and  df5.EID == 'null').show()

# Q.56  Select employee who have worked first ever.
#df3.select('ENAME','EDOJ').orderBy('EDOJ').show()

# Q.57 Display all records falling in odd date
#df3.select('ENAME','EDOJ').filter((col('EDOJ')/2)==0).show()
# Q.58 Display all records falling in even date
# Q.59 Display the vendors whose material is not yet used.
# Q.60 Difference between purchase date and used dsed date of spare part.


#ASCH TP
# ese1=df1.join(df5,df1.CID == df5.CID,'full')
# ese2=df3.join(ese1,df3.EID == ese1.EID,'full').select('CNAME','TYP_VEH','TOTAL')\
#     .groupby('TYP_VEH').max('TOTAL').show()

#df1.filter((df1.CNAME).isin(['CYONA BLAKE','TOM HILL'])).show()


# ESE1=df3.withColumn('new_sal',df3.ESAL * 2).where(df3.ENAME == 'LUIS POPP').show()

# ese1=df3.select('ENAME','ESAL').withColumn("new_sal",\
#             when(df3.ENAME == 'LUIS POPP',df3.ESAL * 2 )\
#                 .otherwise(df3.ESAL)).show()
# data=[{ "column" :'col1',"value": 'val1'},
#       {"column" : 'col2', "value" : 'val2'},
#       {"column" :'col3', "value": 'val3'},
#       {"column" :"col4", "value":'val4'}]
# df=spark.createDataFrame(data=data)
# df.show()


# df.groupBy().pivot('column').agg(max('value')).show()

# df1=spark.read.csv(r"C:\Users\91952\Documents\samplee.csv",inferSchema=True,header=True)
# df1.show()


data2=[(1,2,3),(4,5,6),(7,8,9)]
trans=map(list,zip(*data2))
#print(trans)
# rdd1=spark.sparkContext.parallelize(data2)
df_trans = spark.createDataFrame(trans)
# df_trans.show()

# data1 = [([1,2,3],[4,5,6],[7,8,9])]
# cols = ["col_1","col_2","col_3"]
#
# trsnsdf = spark.createDataFrame(data1,cols)
# trsnsdf.show()
#
#
# trsnsdf.withColumn("ab",arrays_zip("col_1","col_2","col_3"))\
#         .select(explode(col("ab")).alias("tbc"))\
#         .select(col("tbc.col_1"),col("tbc.col_2"),col("tbc.col_3")).show()
#
#


# df.show(truncate=False)

# df = df1.select("a","b").groupby().sum().show()


# df1.withColumn("bc" , arrays_zip("a","b","c"))\
#     .select(explode(col("bc")).alias("tbc")).show()
#     #.select(col("tbc.a"),col("tbc.b"),col("tbc.c"))\
#     #.show()



#df2=df1.groupBy().pivot('a').agg(max('b',)).show()
# df3=df1.groupBy().pivot('C').agg(max('C'))
# df2.show()
# df3.show()
# df4 = df3.join(df2,df3['_'])
# df4.show()
# df1.withColumn("combColumn", concat("df2","df3")).show()
# df1.withColumn("combColumn", concat("_2","_3")).show()



# data1=[(1,None,None),(None,1,None),(None,None,1)]
# schema1=["A","B","C"]
# df=spark.createDataFrame(data=data1,schema=schema1)
# df.show()
#
# df1=df.withColumn('column',coalesce(df['A'],df['B'],df['C'])).select('column')
# df1.show()


# data1=[(1,['a','b']),\
#        (2,['a']),\
#        (3,['a','b','c'])]
# schema1=["num1","num2"]
# df1=spark.createDataFrame(data=data1,schema=schema1)
# df1.show()
#
# df1.select(df1.num1,explode(df1.num2)).show()


# data1=[('John','Torato'),('Jack','Ley')]
# schema1=["fname","lname"]
# df=spark.createDataFrame(data=data1,schema=schema1)
# df.show()
#
# df.createOrReplaceTempView("ext")
# # df_new=df.withColumn('new_string', concat(df.fname.substr(1,1),
# #                                    df.lname.substr(1, 1)))
# # df_new.show()
#
# ese=spark.sql("select fname,lname, substr(fname,1,1) || ''|| substr(lname,1,1) AS Lname from ext").show()





# data1=[(1,),(1,),(1,),(1,),(0,),(0,),(0,),(0,)]
# df=spark.createDataFrame(data1).toDF("col")
# df.show()
#
# windowSpec  = Window.partitionBy("col").orderBy("col")
#
# df1=df.withColumn("row_number",row_number().over(windowSpec))
# df1.orderBy("row_number").show()




# df=spark.read.csv(r"C:\Users\91952\Documents\VQUE.csv",header=True,inferSchema=True)
# df.show()
# df.createOrReplaceTempView("vq")
#
# df.orderBy(df.emp_name).groupby("d_no").agg(collect_list("emp_name").alias("name")).sort("d_no").show()
#


# df=spark.read.csv(r"C:\Users\91952\Documents\justq.csv",header=True,inferSchema=True)
# df.show()
#
# df.createOrReplaceTempView("emp")


# spark.sql("select * from emp").show()
# ese=spark.sql("select b.Id,b.name,b.mgrId,e.Id AS mgr1Id,e.name AS mgrname  from emp b join emp e on b.Id == e.mgrID").show()
#spark.sql("select a.Id , a.name , b.name from emp a right outer join emp b on a.Id=b.mgrId").show()
# spark.sql("select * ")






# data1=[('Male',),('Female',)]
# columns =StructType([StructField ("Gender", StringType())])
# df=spark.createDataFrame(data=data1,schema=columns)
# df.show()

# df.select(df.Gender).withColumn("Category", when(df.Gender == "Male","M")
#                                  .when(df.Gender == "Female","F")
#                                  .otherwise(df.Gender)).show()
#

#df.select(df.Gender).withColumn("Category",df.Gender.substr(1,1)).show()


# df=spark.read.csv(r"C:\Users\91952\Documents\age.csv",header=True,inferSchema=True)
# df.show()
#
# df.select('*').withColumn("agee",when(df.Age > 50 ,"N")
#                     .when(df.Age < 50 ,"Y")
#                     .otherwise(df.Age)).show()


# Year, month, sales, modified_date
# 2020, 1, 1200, 01-12-2020
# 2020, 1, 1000, 05-12-2020
# 2020, 1, 1500, 05-12-2020
# 2020, 2, 800, 01-12-2020
# 2020, 2, 1200, 05-12-2020
# 2020, 2, 1000, 01-12-2020

# data1=[(2020,1, 1200, "01-12-2020"),
#        (2020, 1, 1000, "05-12-2020"),
#        (2020, 1, 1500, "05-12-2020"),
#        (2020, 2, 800, "01-12-2020"),
#        (2020, 2, 1200, "05-12-2020"),
#        (2020, 2, 1000, "01-12-2020")]
#
# schema1=StructType([
#                       StructField('year', IntegerType()),
#                       StructField('month', IntegerType()),
#                       StructField('SALES', IntegerType()),
#                       StructField('modified_date', StringType())
#                   ])
# df=spark.createDataFrame(data=data1,schema=schema1)
# #df.printSchema()
# #df.show()
#
# ese1 = df.withColumn('to_date',to_date(df['modified_date'],'dd-MM-yyyy'))
# #ese1.printSchema()
# #ese1.show()

#df2=ese1.select('*').groupby('to_date').max('sales').show()

# lst=[1,2,3,4,5,6,7,8,9]
# rdd1=spark.sparkContext.parallelize(lst)
# print(rdd1.collect())
# rdd2=rdd1.filter(lambda x : x%2==0)
# print(rdd2.collect())

DATA1=[(1,),(2,),(3,),(4,),(5,),(6,)]
# DATA2=[('a',),('b',),('c',),('d',)]
# schema1=StructType([StructField("COL1",IntegerType()) ])
# #schema2=StructType([StructField("COL2",StringType()) ])

# df1=spark.createDataFrame(data=DATA1,schema=schema1)
# df1.show()
#
# df2 = spark.createDataFrame(DATA2).toDF("col2")
# df2.show()
#
# Window_Spec  = Window.orderBy("COL1")
# Window_Spec1  = Window.orderBy("col2")
#
#
#
# df3=df1.withColumn("row_number",row_number().over(Window_Spec))
# df4=df2.withColumn("row_number",row_number().over(Window_Spec1))
#
# df5=df3.join(df4,df3.row_number==df4.row_number,'full').show()
















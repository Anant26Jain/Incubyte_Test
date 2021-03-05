import cx_Oracle
class Pull_Data:
    print("Establishing Connection..")
    connection=cx_Oracle.connect("SYSTEM","12345678","localhost:1521/xe")
    print("Connected")
    cursor=connection.cursor()
    f = open("Data.txt","r")
    print("Reading File..")
    lines=f.readlines()
    print("Empty Stage Table")
    cursor.execute("DELETE FROM CUSTOMERS_STAGE") 
    print("Inserting Data In Stage Table From File")
    count=0
    for x in lines:
        result=x.strip().split('|')
        if(result[1].upper()=="H"):
           continue
        elif(result[1].upper()=="D"):
            count=count+1
            cursor.execute("INSERT INTO CUSTOMERS_STAGE VALUES('"+result[2]+"','"+result[3]+"','"+result[4]+"','"+result[5]+"','"+result[6]+"','"+result[7]+"','"+result[8]+"','"+result[9]+"','"+result[10]+"','"+result[11]+"')")
    print("Data Inserted Total Rows Processed: "+str(count))
    f.close()
    connection.commit() 
    cursor.execute("INSERT INTO CUSTOMERS_INDIA(SELECT STG.NAME,STG.CUST_ID,TO_DATE(STG.OPEN_DT,'YYYYMMDD'),TO_DATE(STG.CONSUL_DT,'YYYYMMDD'),STG.VAC_ID,STG.DR_NAME,STG.STATE,STG.COUNTRY,NULL,TO_DATE(STG.DOB,'DDMMYYYY'),STG.FLAG FROM CUSTOMERS_STAGE STG LEFT JOIN CUSTOMERS_INDIA TGT ON STG.NAME=TGT.CUSTOMER_NAME WHERE TGT.CUSTOMER_NAME IS NULL AND STG.COUNTRY='IND' AND TRIM(STG.CUST_ID) IS NOT NULL AND TRIM(STG.OPEN_DT) IS NOT NULL)") 
    print("Data Loaded in Target Table")
    connection.commit() 
    f.close()
    cursor.close()
    connection.close()
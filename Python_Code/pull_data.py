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
    cursor.execute("INSERT INTO COUNTRIES(SELECT STG.COUNTRY,0 FROM CUSTOMERS_STAGE STG LEFT JOIN COUNTRIES TGT ON UPPER(STG.COUNTRY)=UPPER(TGT.COUNTRY) WHERE TGT.COUNTRY IS NULL)")
    connection.commit()
    cursor.execute("SELECT COUNTRY FROM COUNTRIES WHERE CREATED=0")
    result=cursor.fetchall()
    for x in result:
        cursor.execute("CREATE TABLE CUSTOMERS_"+x[0]+"(CUSTOMER_NAME VARCHAR(255) PRIMARY KEY,CUSTOMER_ID VARCHAR(18) NOT NULL,CUSTOMER_OPEN_DATE DATE NOT NULL,LAST_CONSULTED_DATE DATE,VACCINATION_TYPE CHAR(5),DOCTOR_CONSULTED CHAR(255),STATE CHAR(5),COUNTRY CHAR(5),POST_CODE INT,DATE_OF_BIRTH DATE,ACTIVE_CUSTOMER CHAR(1))")
        cursor.execute("UPDATE COUNTRIES SET CREATED=1 WHERE COUNTRY='"+x[0]+"'")
        connection.commit()
        print("Table for "+x[0]+" created")
    cursor.execute("SELECT COUNTRY FROM COUNTRIES")
    result=cursor.fetchall()
    for x in result:
        cursor.execute("INSERT INTO CUSTOMERS_"+x[0]+"(SELECT STG.NAME,STG.CUST_ID,TO_DATE(STG.OPEN_DT,'YYYYMMDD'),TO_DATE(STG.CONSUL_DT,'YYYYMMDD'),STG.VAC_ID,STG.DR_NAME,STG.STATE,STG.COUNTRY,NULL,TO_DATE(STG.DOB,'DDMMYYYY'),STG.FLAG FROM CUSTOMERS_STAGE STG LEFT JOIN CUSTOMERS_"+x[0]+" TGT ON STG.NAME=TGT.CUSTOMER_NAME WHERE TGT.CUSTOMER_NAME IS NULL AND STG.COUNTRY='"+x[0]+"' AND TRIM(STG.CUST_ID) IS NOT NULL AND TRIM(STG.OPEN_DT) IS NOT NULL)") 
        print("Data Loaded in "+x[0]+"Table")
    connection.commit() 
    f.close()
    cursor.close()
    connection.close()
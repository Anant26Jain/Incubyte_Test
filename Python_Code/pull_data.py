import cx_Oracle
class Pull_Data:
    connection=cx_Oracle.connect("SYSTEM","12345678","localhost:1521/xe")
    cursor=connection.cursor()
    f = open("Data.txt","r")
    lines=f.readlines()
    cursor.execute("DELETE FROM CUSTOMERS_STAGE") 
    for x in lines:
        result=x.strip().split('|')
        if(result[1].upper()=="H"):
           continue
        elif(result[1].upper()=="D"):
            cursor.execute("INSERT INTO CUSTOMERS_STAGE VALUES('"+result[2]+"','"+result[3]+"',(TO_DATE('"+result[4]+"','YYYYMMDD'))"+",(TO_DATE('"+result[4]+"','YYYYMMDD'))"+",'"+result[6]+"','"+result[7]+"','"+result[8]+"','"+result[9]+"',(TO_DATE('"+result[4]+"','YYYYMMDD'))"+",'"+result[11]+"')")
    connection.commit()        
    f.close()
    cursor.close()
    connection.close()
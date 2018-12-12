from PyPDF2 import PdfFileReader, PdfFileWriter
import os
import pyhdb #Python Hana DB connector

#!Establish DB Connection
def db_connection():
    connection = pyhdb.connect(
        host = "host",
        port = 00000,
        user = "dbUser",
        password = "secret"
    )
    return connection

#!Split parent pdf and store them in a directory
def split_pdf(file_name):
    #read file and get total page number
    pdf_file = open(file_name, 'rb')
    file_reader = PdfFileReader(pdf_file)
    total = file_reader.getNumPages()
    print("File name: ", file_name)
    print("Total page(s): ", total)

    #create folder if doesn't exist
    folder = os.path.splitext(file_name)[0]
    if not os.path.exists(folder):
        os.mkdir(folder)
        print("Created folder named ", folder)

    #split pdf pages
    remaining = True
    while remaining:
        for i in range(0, total):
            file_writer = PdfFileWriter()
            file_writer.addPage(file_reader.getPage(i))
            page_name = folder + "_page_" + str(i+1) + ".pdf"
            new_page_path = os.path.join(folder, page_name)
            print("new page: " , page_name)
            new_page = open(new_page_path, 'wb')
            file_writer.write(new_page)
        remaining = False

    return remaining

#!Upload single pdfs to the database
def upload_to_hdb(conn, cursor, file_name, pdf_type):
    parentPDF = os.path.splitext(file_name)[0] #parent pdf name
    file_dir = os.path.abspath(parentPDF)
    total = len(os.listdir(file_dir)) #total number of files

    try:
        if conn and conn.isconnected():
            for i in range(0, total):
                pageName = parentPDF + "_page_" + str(i+1) + ".pdf"
                pagePath = os.path.join(parentPDF,  pageName)
                print("adding: ", pageName)
                pageFile = open(pagePath, 'rb')
                pageFile = pageFile.read()
                #! Change to your own database schema name
                cursor.execute("INSERT INTO SCHEMA.DEMO_TABLE VALUES(?,?,?,?)", (parentPDF, pageName, pdf_type, pageFile))

            print("Insert complete, started commit...")
            conn.commit()
            print("Successful commit")

    except Exception as err:
        print("Error: " + str(err))

#!Main function 
def main():
    conn = db_connection()
    file_name = "Sample.pdf"
    pdf_type = 1

    try:
        if conn and conn.isconnected():
            print("Connection established.")
            conn.setautocommit(False)
            cursor = conn.cursor()

            print("Attempting to split parent pdf")
            remaining = split_pdf(file_name)
            print("Page remaining? ", remaining)

            if not remaining:
                print("Preparing to insert into database")
                upload_to_hdb(conn, cursor, file_name, pdf_type)
        else:
            print("Connection failed.")

    except Exception as err:
        print("Error: " + str(err))

if __name__ == '__main__':
    main()

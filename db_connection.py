#-------------------------------------------------------------------------
# AUTHOR: Grecia Alvarado
# FILENAME: db_connection.py
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: 8 hr
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2
from psycopg2.extras import RealDictCursor

def connectDataBase():
    DB_NAME = "CPP"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    try:
        conn = psycopg2.connect(database = DB_NAME,
                                user = DB_USER,
                                password = DB_PASS,
                                host = DB_HOST,
                                port = DB_PORT)
        return conn
    except:
        print("Database not connected successfully")

# changed from signature with dupe parameter:
# def createCategory(cur, cur, catId, catName):
def createCategory(cur, catId, catName):

    # Insert a category in the database
    sql = "Insert into categories (id, name) Values (%s, %s)"
    recset = [catId, catName]
    cur.execute(sql, recset)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    cur.execute("select * from categories where name like %(docCat)s",
                {'docCat': '%{}%'.format(docCat)})
    category = cur.fetchall()

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    sql = "Insert into documents (docId, category, docText, docTitle, num_chars, docDate) " \
          "Values (%s, %s, %s, %s, %s, %s)"
    
    num_chars = len(docText.strip('.,?!'))
    recset = [docId, category, docText, docTitle, num_chars, docDate]

    cur.execute(sql, recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    terms = [term.strip('.,?!').lower() for term in docText.split()] 
    for term in terms:
        cur.execute("select id from terms where term like %s", term)
        term_id = cur.fetchone()

        # i have terms as its own table in #1
        if term_id is None:
            cur.execute("Insert into terms (term, num_chars) Values (%s, %s)", (term, len(term)))


    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    term_counts = {}
    for term in terms:
        term_counts[term] = terms.count(term)

        cur.execute("select id from terms where term like %s", term)

        recset = [term_id, docId, term_counts[term]]
        cur.execute("Intsert into terms (term, docId, count) Values (%s, %s, %s)", recset)

    recset = [docId, category, docText, docTitle, len(docText), docDate]
    cur.execute("Insert into documents (id, category_id, text, title, num_chars, date) Values (%s, %s, %s, %s, %s, %s)",
                recset)
    cur.commit()

    

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    cur.execute("select term from documents where docId like %s", docId)
    term_ids = cur.fetchall()
    for term_id in term_ids:
            cur.execute("Delete from documents where term_id = %s", term_id)
            # 1.2:
            cur.execute("select count(*) from terms where term_id = %s", term_id)
            count = cur.fetchone()[0]
            if count == 0:
                cur.execute("Delete from terms where id = %s", term_id) # deleteComment uses = not like

    # 2 Delete the document from the database
    sql = "Delete from documents where id = %(docId)s"
    cur.execute(sql, {'id': docId})

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)
    cur.commit()

def getIndex(cur):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
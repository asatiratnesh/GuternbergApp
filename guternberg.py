#from .db_helper import db_ssh_query
from os import environ
import flask
from flask import Flask, request, abort
from flask_cors import CORS
from flask import jsonify, make_response
from flaskext.mysql import MySQL

app = flask.Flask(__name__)

app.config["FLASK_APP"] = environ.get("FLASK_APP")
app.config["FLASK_ENV"] = environ.get("FLASK_ENV")
app.config["DEBUG"] = environ.get("FLASK_DEBUG")
app.config["SECRET_KEY"] = environ.get("SECRET_KEY")
app.config["FLASK_RUN_HOST"] = environ.get("FLASK_RUN_HOST")
app.config["FLASK_RUN_PORT"] = environ.get("FLASK_RUN_PORT")
app.config['JSON_SORT_KEYS'] = False

mysql = MySQL()
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = 'root'
app.config['MYSQL_DATABASE_DB'] = 'guternberg_db'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'
mysql.init_app(app)


def data_filter(input_json, query, cursor, page_no):
    if "book_id" in input_json["filter"]:
        book_ids = input_json["filter"]["book_id"]
        filter_query_tail = ''
        for idx, book_id in enumerate(book_ids):
            if len(book_ids) == 1:
                filter_query_tail = " t1.gutenberg_id = '"+str(book_id)+"' "
            elif len(book_ids) == idx+1:
                filter_query_tail = filter_query_tail +" t1.gutenberg_id = '"+str(book_id)+"' " 
            else:
                filter_query_tail = " t1.gutenberg_id = '"+str(book_id)+"' OR " + filter_query_tail

        filter_query = '''where '''+filter_query_tail+''' ORDER BY
            t1.download_count desc'''
        cursor.execute(query+filter_query)
    if "language" in input_json["filter"]:
        languages = input_json["filter"]["language"]
        filter_query_tail = ''

        for idx, language in enumerate(languages):
            if len(languages) == 1:
                filter_query_tail = " t13.code = '"+str(language)+"' "
            elif len(languages) == idx+1:
                filter_query_tail = filter_query_tail +" t13.code = '"+str(language)+"' " 
            else:
                filter_query_tail = " t13.code = '"+str(language)+"' OR " + filter_query_tail

        filter_query = '''where t1.gutenberg_id IN (
            SELECT gutenberg_id FROM (SELECT ROW_NUMBER() OVER
            (ORDER BY t1.download_count desc) AS RowNumber,
            t1.gutenberg_id from books_book as t1
            inner join books_book_languages as t12 on t1.gutenberg_id=t12.book_id
            inner join books_language as t13 on t12.language_id=t13.id
            where  '''+filter_query_tail+''' GROUP BY t1.gutenberg_id) _tmpView1 
            WHERE RowNumber > %s and RowNumber <= %s
            )  ORDER BY t1.download_count desc'''
        cursor.execute(query+filter_query, (page_no, page_no+25))
    if "mine_type" in input_json["filter"]:
        mine_types = input_json["filter"]["mine_type"]  
        filter_query_tail = ''

        for idx, mine_type in enumerate(mine_types):
            if len(mine_types) == 1:
                filter_query_tail = " t1.media_type = '"+mine_type+"' "
            elif len(mine_types) == idx+1:
                filter_query_tail = filter_query_tail +" t1.media_type = '"+mine_type+"' " 
            else:
                filter_query_tail = " t1.media_type = '"+mine_type+"' OR " + filter_query_tail

        filter_query = '''where t1.gutenberg_id IN (
            SELECT gutenberg_id FROM (SELECT ROW_NUMBER() OVER 
            (ORDER BY t1.download_count desc) AS RowNumber,
            t1.gutenberg_id from books_book as t1 
            where '''+filter_query_tail+''' GROUP BY t1.gutenberg_id) _tmpView1 
            WHERE RowNumber > %s and RowNumber <= %s
            ) ORDER BY t1.download_count desc'''
        print(filter_query)
        cursor.execute(query+filter_query, (page_no, page_no+25))
    if "topic" in input_json["filter"]:
        topics = input_json["filter"]["topic"]  
        filter_query_tail = ''

        for idx, topic in enumerate(topics):
            if len(topics) == 1:
                filter_query_tail = " t13.name like '%%"+topic+"%%' or t15.name like '%%"+topic+"%%'"
            elif len(topics) == idx+1:
                filter_query_tail = filter_query_tail +" t13.name like '%%"+topic+"%%'  or t15.name like '%%"+topic+"%%'" 
            else:
                filter_query_tail = " t13.name like '%%"+topic+"%%' or t15.name like '%%"+topic+"%%' OR " + filter_query_tail

        filter_query = '''where t1.gutenberg_id IN (
            SELECT gutenberg_id FROM (SELECT ROW_NUMBER() OVER
            (ORDER BY t1.download_count desc) AS RowNumber,
            t1.gutenberg_id from books_book as t1
            inner join books_book_subjects as t12 on t1.gutenberg_id=t12.book_id
            inner join books_subject as t13 on t12.subject_id=t13.id
            inner join books_book_bookshelves as t14 on t1.gutenberg_id=t14.book_id
            inner join books_bookshelf as t15 on t14.bookshelf_id=t15.id
            where '''+filter_query_tail+'''
            GROUP BY t1.gutenberg_id) _tmpView1 
            WHERE RowNumber > %s and RowNumber <= %s
            )  ORDER BY t1.download_count desc'''
        cursor.execute(query+filter_query, (page_no, page_no+25))
    if "author" in input_json["filter"]:
        authors = input_json["filter"]["author"]
        filter_query_tail = ''

        for idx, author in enumerate(authors):
            if len(authors) == 1:
                filter_query_tail = " t13.name like '%%"+author+"%%' "
            elif len(authors) == idx+1:
                filter_query_tail = filter_query_tail +" t13.name like '%%"+author+"%%'" 
            else:
                filter_query_tail = " t13.name like '%%"+author+"%%' OR " + filter_query_tail

        filter_query = '''where t1.gutenberg_id IN (
            SELECT gutenberg_id FROM (SELECT ROW_NUMBER() OVER
            (ORDER BY t1.download_count desc) AS RowNumber,
            t1.gutenberg_id from books_book as t1
            inner join books_book_authors as t12 on t1.gutenberg_id=t12.book_id
            inner join books_author as t13 on t12.author_id=t13.id
            where '''+filter_query_tail+'''
            GROUP BY t1.gutenberg_id) _tmpView1 
            WHERE RowNumber > %s and RowNumber <= %s
            )  ORDER BY t1.download_count desc'''
        cursor.execute(query+filter_query, (page_no, page_no+25))
    if "title" in input_json["filter"]:
        titles = input_json["filter"]["title"]
        filter_query_tail = ''

        for idx, title in enumerate(titles):
            if len(titles) == 1:
                filter_query_tail = " t1.title like '%%"+title+"%%' "
            elif len(titles) == idx+1:
                filter_query_tail = filter_query_tail +" t1.title like '%%"+title+"%%'" 
            else:
                filter_query_tail = " t1.title like '%%"+title+"%%' OR " + filter_query_tail
        filter_query = '''where t1.gutenberg_id IN (
            SELECT gutenberg_id FROM (SELECT ROW_NUMBER() OVER
            (ORDER BY t1.download_count desc) AS RowNumber,
            t1.gutenberg_id from books_book as t1
            where '''+filter_query_tail+'''
            GROUP BY t1.gutenberg_id) _tmpView1 
            WHERE RowNumber > %s and RowNumber <= %s
            )  ORDER BY t1.download_count desc'''
        cursor.execute(query+filter_query, (page_no, page_no+25))
    

@app.route('/search_books', methods=['POST'])
def search_books():
    try:
        page_no = request.args.get('page')
        if page_no is None:
            page_no = 0
        else:
            page_no = int(page_no)*25 - 25
        conn = mysql.connect()
        cursor = conn.cursor()
        query = '''SELECT t1.gutenberg_id as book_id, t1.title as book_title,
            t3.name as author_name, t3.birth_year as author_birth_year, 
            t3.death_year as author_death_year, t5.name as genre,
            t7.code as language_code, t9.name as subject,
            t10.url as download_link, t10.mime_type as media_type
            from books_book as t1 
            left join books_book_authors as t2 on t1.gutenberg_id=t2.book_id
            left join books_author as t3 on t3.id=t2.author_id
            left join books_book_bookshelves as t4 on t1.gutenberg_id=t4.book_id
            left join books_bookshelf as t5 on t5.id=t4.bookshelf_id
            left join books_book_languages as t6 on t1.gutenberg_id=t6.book_id
            left join books_language as t7 on t7.id=t6.language_id
            left join books_book_subjects as t8 on t1.gutenberg_id=t8.book_id
            left join books_subject as t9 on t9.id=t8.subject_id
            left join books_format as t10 on t1.gutenberg_id=t10.book_id
            '''
        
        filter_query = '''where t1.gutenberg_id IN (
            SELECT gutenberg_id FROM (SELECT ROW_NUMBER() OVER 
            (ORDER BY t1.download_count desc) AS RowNumber,
            t1.gutenberg_id from books_book as t1 ) _tmpView1 
            WHERE RowNumber > %s and RowNumber <= %s
            ) ORDER BY t1.download_count desc'''

        input_json = request.get_json(force=True)

        if "filter" in input_json:
            data_filter(input_json, query, cursor, page_no)
        else:
            cursor.execute(query+filter_query, (page_no, page_no+25))

        rv = cursor.fetchall()
        json_data=[]
        result_dict = {}
        book_title = set()
        author_name = set()
        author_birth_year = set()
        author_death_year = set()
        genre = set()
        language_code = set()
        subject = set()
        download_link = set()
        media_type = set()

        for result in rv:
            if result[0] in result_dict:
                book_title.add(result[1])
                author_name.add(result[2])
                author_birth_year.add(result[3])
                author_death_year.add(result[4])
                genre.add(result[5])
                language_code.add(result[6])
                subject.add(result[7])
                download_link.add(result[8])
                media_type.add(result[9])
                result_dict[result[0]] = {
                    "book_title": list(book_title),
                    "author_name": list(author_name),
                    "author_birth_year": list(author_birth_year),
                    "author_death_year": list(author_death_year),
                    "genre": list(genre),
                    "language_code": list(language_code),
                    "subject": list(subject),
                    "download_link": list(download_link),
                    "media_type": list(media_type)}
            else:
                if not len(book_title):
                    book_title.add(result[1])
                else:
                    book_title.clear()
                if not len(author_name):
                    author_name.add(result[2])
                else:
                    author_name.clear()
                if not len(author_birth_year):
                    author_birth_year.add(result[3])
                else:
                    author_birth_year.clear()
                if not len(author_death_year):
                    author_death_year.add(result[4])
                else:
                    author_death_year.clear()
                if not len(genre):
                    genre.add(result[5])
                else:
                    genre.clear()
                if not len(language_code):
                    language_code.add(result[6])
                else:
                    language_code.clear()
                if not len(subject):
                    subject.add(result[7])
                else:
                    subject.clear()
                if not len(download_link):
                    download_link.add(result[8])
                else:
                    download_link.clear()
                if not len(media_type):
                    media_type.add(result[9])
                else:
                    media_type.clear()
                result_dict[result[0]] = {
                    "book_title": list(book_title),
                    "author_name": list(author_name),
                    "author_birth_year": list(author_birth_year),
                    "author_death_year": list(author_death_year),
                    "genre": list(genre),
                    "language_code": list(language_code),
                    "subject": list(subject),
                    "download_link": list(download_link),
                    "media_type": list(media_type)}

        return make_response(jsonify(result_dict), 200)
    except Exception as e:
        app.logger.error('ERROR: ' + str(e))
        response = {
            "message": "Error",
            "error": str(e)
        }
        return make_response(jsonify(response), 400)


if __name__ == '__main__':
    app.run('localhost', 5002)

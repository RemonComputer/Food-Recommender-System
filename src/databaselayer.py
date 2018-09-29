from cassandra.cluster import Cluster
from pandas import DataFrame
import pandas
import re


server_ip = '127.0.0.1'
keyspace_name = 'foodrec'
data_files_path = '../data/'
meals_names = ['Breakfast', 'Lunch', 'Dinner']
separator = ';'

cluster = None
session = None

def initialize_database():
    print("Creating database {}".format(keyspace_name))
    session.execute("""
           CREATE KEYSPACE %s
           WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
           """ % keyspace_name)
    session.set_keyspace(keyspace_name)
    print("Creating question table")
    session.execute("""
            CREATE TABLE question (
                shorthand text,
                question_text text,
                answers list<text>,
                type text,
                insertion_time timestamp,
                PRIMARY KEY(shorthand, insertion_time)
            ) WITH CLUSTERING ORDER BY (insertion_time ASC) 
            """)
    print("Creating weights dictionary")
    session.execute("""
                CREATE TABLE weights_dictionary (
                    label text PRIMARY KEY,
                    weight float
                )
                """)
    print("Creating day_meals type")
    session.execute("""
            CREATE TYPE day_meals (
                Breakfast_id UUID,
                Lunch_id UUID,
                Dinner_id UUID
            )
    """)
    print("Creating user table")
    session.execute("""
            CREATE TABLE user (
                id UUID PRIMARY KEY,
                username text,
                password text,
                is_admin boolean,
                answers map<text, frozen<list<text>>>,
                history list<frozen<day_meals>>
            )
            """)
    print("Creating recipe table")
    session.execute("""
            CREATE TABLE recipe (
                id UUID PRIMARY KEY,
                meal text,
                title text,
                calories int,
                carbs int,
                fats int,
                proteins int,
                prep int,
                cook int,
                description text,
                directions list<text>,
                ingredients list<text>,
                image text,
                link text
            )
            """)
    print("filling recipe table")
    for meal_name in meals_names:
        file_path = data_files_path + meal_name + '.xlsx'
        data_frame = pandas.read_excel(file_path)
        for idx, row in data_frame.iterrows():
            print('Adding meal {} Recipe: {}'.format(meal_name, row['Title']))
            insert_recipe(meal=meal_name,
                          title=row['Title'],
                          calories= covert_text_to_int(row['Calories']),
                          carbs= covert_text_to_int(row['Carbs']),
                          fats=covert_text_to_int(row['Fat']),
                          proteins= covert_text_to_int(row['Protein']),
                          prep= covert_text_to_int(row['Prep']),
                          cook= covert_text_to_int(row['Cook']),
                          description=row['Description'],
                          directions=row['Directions'].split(separator),
                          ingredients=row['Ingredients'].split(separator),
                          image=row['Image'],
                          link= row['link']
                          )
    print('Finished Adding Recipes')
    print('Creating Admin user')
    add_user('admin', 'admin')
    print('Adding questions')
    add_question('gender', 'Select your gender', ['Male', 'Female'],"ONE_CHOICE")
    add_question('age', 'How old are you?', [], "NUMBER")
    add_question('height', 'How tall are you? (cm)', [], "NUMBER")
    add_question('weight', 'What is your weight? (kg)', [], "NUMBER")
    add_question('activity', 'Physical activity', ['Do not exercise', 'Exercise lightly one to three times per week',
                                                   'Exercise three to five days per week', 'Exercise six or seven days per week',
                                                   'Exercise seven days a week and also have a physically demanding job'],"ONE_CHOICE")
    add_question('meat', "What meat types you <span class=\"important-text\">don't like</span>?", ['chicken', 'beef', 'fish', 'turkey', 'pork'],"MULTIPLE_CHOICE")
    add_question('veggies', "What vegetable types you <span class=\"important-text\">don't like</span>?", ['potato', 'rice', 'black beans', 'rolled oats', 'quinoa', 'sweet potato', 'cauliflower'],
                 "MULTIPLE_CHOICE")
    add_question('fruits', "What fruit types you <span class=\"important-text\">don't like</span>?", ['orange', 'apple', 'banana', 'pineapple', 'grapefruit'],"MULTIPLE_CHOICE")
    add_question('products', "What milk products you <span class=\"important-text\">don't like</span>?", ['egg', 'nuts', 'yogurt', 'soy milk', 'cheese', 'tofu', 'tempeh', 'cottage cheese'],"MULTIPLE_CHOICE")
    # add_question('day', 'Describe a typical day for you', ['at the office', 'at the office, but I go out on a regular basis',
    #                                                        'I spend, the better part of the day on foot', 'manual labor',
    #                                                        'I mostly stay at home'],"ONE_CHOICE")
    # add_question('eating', 'Which of the following is true for you?', ['I do no get enough sleep', 'I eat late eat night',
    #                                                                    'I consume a lot of salt', 'I cannot give up eating sweets',
    #                                                                    'I love soft drinks', 'None of the above'],"MULTIPLE_CHOICE")
    #add_question('target_weight', 'What is your target weight? (kg)', [],"NUMBER")
    print('Adding labels in weight dictionary')
    add_label_and_weight('Do not exercise', 1.2)
    add_label_and_weight('Exercise lightly one to three times per week', 1.375)
    add_label_and_weight('Exercise three to five days per week', 1.55)
    add_label_and_weight('Exercise six or seven days per week', 1.725)
    add_label_and_weight('Exercise seven days a week and also have a physically demanding job', 1.9)

def insert_recipe(meal, title, calories, carbs, fats, \
                  proteins, prep, cook, description, directions, \
                  ingredients, image, link):
    prepared = session.prepare("""
    INSERT INTO recipe (id, meal, title, calories, carbs, fats, proteins, prep, cook,
    description, directions, ingredients, image, link) VALUES (now(),?,?,?,?,?,?,?,?,?,?,?,?,?)
    """)
    prepared_binded = prepared.bind((meal, title, calories, carbs, fats, proteins, prep, cook, \
                                     description, directions, ingredients, image, link))
    session.execute(prepared_binded)

def is_database_exists():
    rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    if keyspace_name in [row[0] for row in rows]:
        return True
    return False

def covert_text_to_int(text):
    if type(text) != str:
        return text
    new_text = re.sub(r'[A-Za-z \.]+', '', text)
    result = int(new_text)
    return result

def add_day_meals(username, breakfast_id, lunch_id, dinner_id):
    # user_object = get_user_by_username(username)
    # prepared = None
    # if user_object.history == None:
    #     prepared = session.prepare("""
    #                     UPDATE user SET history = [{Breakfast_id: ?,
    #                                                 Lunch_id: ?,
    #                                                 Dinner_id: ?}]
    #                     WHERE username = ?
    #                     """)
    # else:
    #     prepared = session.prepare("""
    #             UPDATE user SET history = history + [{Breakfast_id: ?,
    #                                                   Lunch_id: ?,
    #                                                   Dinner_id: ?}]
    #             WHERE username = ?
    #             """)
    # prepared_binded = prepared.bind((breakfast_id, lunch_id, dinner_id, username))
    # session.execute(prepared_binded)
    userid = session.execute(session.prepare("SELECT id FROM user WHERE username = ? ALLOW FILTERING").bind((username,)))[0][0]
    query = """UPDATE user SET history = history + [{{Breakfast_id: {},
                                                       Lunch_id: {},
                                                       Dinner_id: {}}}] 
                 WHERE id = {}
                 """.format(breakfast_id, lunch_id, dinner_id, userid)
    session.execute(query)

def  get_user_by_username(username):
    query = session.prepare("SELECT * FROM user WHERE username=? LIMIT 1  ALLOW FILTERING")
    rows = session.execute(query, (username,))
    return rows[0]

def get_weights_dictionary():
    query = "SELECT * FROM weights_dictionary"
    result = session.execute(query)
    return result

def get_meal_recipes(meal):
    prepared = session.prepare("SELECT * FROM recipe WHERE meal = ? ALLOW FILTERING")
    prepared_binded = prepared.bind((meal,))
    rows = session.execute(prepared_binded)
    return rows

def get_recipe_by_id(id):
    prepared = session.prepare("SELECT * FROM recipe WHERE id = ? LIMIT 1  ALLOW FILTERING ")
    prepared_binded = prepared.bind((id,))
    rows = session.execute(prepared_binded)
    return rows[0]

def add_question(shorthand, question, answers, question_type):
    prepared = session.prepare("""
        INSERT INTO question (shorthand, question_text, answers, type, insertion_time) VALUES (?, ?, ?, ?, toTimeStamp(now()))
        """)
    prepared_binded = prepared.bind((shorthand, question, answers, question_type))
    session.execute(prepared_binded)

def add_label_and_weight(label, weight):
    label = str(label)
    weight = float(weight)
    prepared = session.prepare("""
            INSERT INTO weights_dictionary (label, weight) VALUES (?, ?)
            """)
    prepared_binded = prepared.bind((label, weight))
    session.execute(prepared_binded)

def add_answer(username, shorthand, answer):
    userid = session.execute(session.prepare("SELECT id FROM user WHERE username = ? ALLOW FILTERING").bind((username, )))[0][0]
    prepared = session.prepare("""
        UPDATE user SET answers[?] = ? WHERE id = ?
        """)
    prepared_binded = prepared.bind((shorthand, answer, userid))
    session.execute(prepared_binded)

def add_user(username, password):
    prepared = session.prepare("""
        INSERT INTO user (id, username, password, history) VALUES (now(), ?, ?, [])
        """)
    prepared_binded = prepared.bind((username, password))
    session.execute(prepared_binded)

# fetches the questions and answers of some user
def get_questions():
    query = session.prepare("SELECT shorthand, question_text, insertion_tim"
                            "e  FROM question")
    rows = session.execute(query)
    questions_list = list(rows)
    questions_list.sort(key = lambda e:e.insertion_time)
    return [(qa[0], qa[1], "") for qa in questions_list]

def get_question(username, shorthand):
    query = session.prepare("SELECT question_text, answers, type FROM question WHERE shorthand=? ALLOW FILTERING")
    row = session.execute(query, (shorthand, ))[0]
    question = row[0]
    choices = row[1]
    type = row[2]
    query = session.prepare("SELECT answers FROM user WHERE username=? ALLOW FILTERING")
    row = session.execute(query, (username, ))[0]
    answer = None
    if row and row[0]:
        if shorthand in row[0]:
            answer = row[0][shorthand]
        else:
            answer = None
    return (question, answer, choices, type)

def get_shorthands():
    query = session.prepare("SELECT shorthand, insertion_time FROM question")
    rows = session.execute(query)
    result_list = list(rows)
    result_list.sort(key = lambda r:r.insertion_time)
    return [result.shorthand for result in result_list]
    
def check_credentials(username, password):
    query = session.prepare("SELECT * FROM user WHERE username=? and password=? ALLOW FILTERING")
    rows = session.execute(query, (username, password))
    if rows:
        return True
    return False
    
def initialize_data_base_layer():
    global cluster
    global session
    cluster = Cluster([server_ip])
    session = cluster.connect()
    if is_database_exists() == False:
        initialize_database()
    session.set_keyspace(keyspace_name)


#executes this function when module initializes
initialize_data_base_layer()

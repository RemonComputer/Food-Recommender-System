#!/usr/bin/python3

import databaselayer as db
import recommender as rc
from flask import Flask, redirect, render_template, request, session, url_for, send_from_directory
import webbrowser

app = Flask(__name__, static_url_path='')
server_ip = '0.0.0.0'
server_port = 5000

@app.route('/index.html')
def index():
    return render_template('index.html')

@app.route('/recommender.html')
def recommender_page():
    username = session['username']
    (breakfast_id, lunch_id, dinner_id) = rc.recommend_meals(username)
    db.add_day_meals(username, breakfast_id, lunch_id, dinner_id)
    breakfast = db.get_recipe_by_id(breakfast_id)
    lunch = db.get_recipe_by_id(lunch_id)
    dinner = db.get_recipe_by_id(dinner_id)
    return render_template('recommender.html', breakfast=breakfast, lunch=lunch, dinner=dinner)

@app.route('/signup.html', methods=['GET'])
def signup():
    return render_template('signup.html')

@app.route('/signup/', methods=['POST'])
def signup_post():
    username = request.form.get('username')
    password = request.form.get('password')
    confirm_password = request.form.get('passwordConfirmation')
    if password == confirm_password:
        db.add_user(username, password)
        session.clear()
        session['username'] = username
        #return redirect('/question/', code=302)
        #begin asking questions about the person to reccord them in the database, redirecting the person to the first question
        return redirect('/question/gender/', code=302)
    else:
        return render_template('signup.html', passwordsMatch=False)

@app.route('/login/', methods=['GET'])
def login_get():
    return render_template('login.html')

@app.route('/login/', methods=['POST'])
def login_post():
    username = request.form.get('username')
    password = request.form.get('password')
    if db.check_credentials(username, password):
        session.clear()
        session['username'] = username
        return redirect('/index.html', code=302)
    else:
        return render_template('login.html', success=False)

@app.route('/logout/')
def logout():
    session.clear()
    return redirect('/index.html', code=302)

@app.route('/question/')
def question_list():
    return render_template('questions.html', questions=db.get_questions())

@app.route('/question/<shorthand>/', methods=['GET'])
def question_edit(shorthand):
    if not 'username' in session or session['username'] is None:
        return redirect('/login/', 302)
    question, answer, choices, type = db.get_question(session['username'], shorthand)
    return render_template('question.html', question=question, answer=answer, choices=choices, type=type)
    #return render_template('question.html', question="srsly?", answer="yea", choices=["yea", "no"])

@app.route('/question/<shorthand>/', methods=['POST'])
def question_answer(shorthand):
    if not 'username' in session or session['username'] is None:
        return redirect('/login/', 302)
    # todo: validate shorthand
    (_, _, choices, type) = db.get_question(session['username'], shorthand)
    if type == "NUMBER" or type == "ONE_CHOICE" :
        #one asnwer question
        sane_answer = [request.form.get('answer')]
    else:
        #multiple answers question
        sane_answer = list(set(choices).intersection(request.form.getlist('answer')))
    db.add_answer(session['username'], shorthand, sane_answer)
    #jump to the next question
    shorthands = db.get_shorthands()
    #next_shorthand = None
    next_shorthand_index = shorthands.index(shorthand) + 1
    if next_shorthand_index < len(shorthands):
        next_shorthand = str(shorthands[next_shorthand_index])
        return redirect(url_for('question_edit', shorthand=next_shorthand), 302)
    return redirect('/index.html', code=302)

@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('templates/js', path)

@app.route('/img/<path:path>')
def send_img(path):
    return send_from_directory('templates/img', path)

@app.route('/css/<path:path>')
def send_css(path):
    return send_from_directory('templates/css', path)

@app.route('/fonts/<path:path>')
def send_fonts(path):
    return send_from_directory('templates/fonts', path)

app.secret_key = 'so secret'
if __name__ == '__main__':
    app_url = 'http://{}:{}/index.html'.format(server_ip, server_port)
    webbrowser.open_new_tab(app_url)
    app.run(server_ip, server_port, True)
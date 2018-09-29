import databaselayer as db
from pandas import DataFrame

breakfast_data = None
lunch_data = None
dinner_data = None
weight_dictionary = {}

def get_meal_data(meal):
    query_results = db.get_meal_recipes(meal)
    result_list = list(query_results)
    result = DataFrame(result_list)
    return result

def get_weight_dictionary():
    rows = db.get_weights_dictionary()
    result = {}
    for row in rows:
        result[row.label] = row.weight
    return result

def get_histroy_dataframe(user_object):
    history_list = user_object.history
    if history_list == None:
        return DataFrame(columns=['username', 'Breakfast', 'Lunch', 'Dinner'])
    username = user_object.username
    history_list_formatted = []
    for history_entry in history_list:
        history_list_formatted.append({'username': username,
                                       'Breakfast': history_entry.breakfast_id,
                                       'Lunch': history_entry.lunch_id,
                                       'Dinner': history_entry.dinner_id})
    result = DataFrame(history_list_formatted)
    return result


#Daily calorie calculator
#Not suitable for children and pregnant
#formula for Male
#Reference for activity level https://www.livestrong.com/article/526442-the-activity-factor-for-calculating-calories-burned/
def daily_calorie_male(height, weight, age, activity_level):
    daily_calories = activity_level * (66 + (13.7 * weight) + (5 * height) - (6.8 * age))
    return daily_calories

#formula for female
def daily_calorie_female(height, weight, age, activity_level):
    daily_calories = activity_level * (655 + (9.6 * weight) + (1.8 * height) - (4.7 * age))
    return daily_calories


#method to check if the meal includes unfavourable ingredient or not
#meal ingredients is the list of detailed ingredients from the dataset
#hated_ingredients is the list of the ingredients that user unlike
def check_hated_ingredient(meal_ingredients,hated_ingredients):
    #meal_ingredients = list(meal_ingredients)
    splitted_ing = []
    for ingredient in meal_ingredients:#meal_ingredients.split(';')
        for desc in ingredient.split(' '):
            splitted_ing.append(desc.strip(','))
        for i in hated_ingredients:
            if i in splitted_ing:
                return True
                break
    return False

def get_next_meal_index(meal_id_series, meal_dataframe_data):
    if len(meal_id_series) == 0:
        return 0
    meal_id = list(meal_id_series)[-1]
    found_locations = meal_dataframe_data.index[meal_dataframe_data['id'] == meal_id].tolist()
    accepted_location = (found_locations[0] + 1) % len(meal_dataframe_data)
    return accepted_location

def generate_meal_plan(user_object, calories):
    #global counter
    #counter += 1
    #breakfast_cal = 0
    #lunch_cal = 0
    #dinner_cal = 0
    #breakfast_id = breakfast_data['id'][0]
    #lunch_id = lunch_data['id'][0]
    #dinner_id = dinner_data['id'][0]
    #remaining_cal = 0
    #username = user_object.username
    hated_list = []
    hated_list.extend(list(user_object.answers['meat']))
    hated_list.extend(list(user_object.answers['veggies']))
    hated_list.extend(list(user_object.answers['fruits']))
    hated_list.extend(list(user_object.answers['products']))
    df = get_histroy_dataframe(user_object)
    # if len(df.loc[df['username'] == username]['username']) == 0:
    #     breakfast_index = 0
    #     lunch_index = 0
    #     meal_index = 1
    #     while check_hated_ingredient(breakfast_data['ingredients'][breakfast_index], hated_list):
    #         breakfast_index += 1
    #     while check_hated_ingredient(breakfast_data['ingredients'][lunch_index], hated_list):
    #         lunch_index += 1
    #     breakfast_id = breakfast_data['id'][breakfast_index]
    #     breakfast_cal = breakfast_data['calories'][breakfast_index]
    #     lunch_id = lunch_data['id'][lunch_index]
    #     lunch_cal = lunch_data['calories'][lunch_index]
    #     remaining_cal = calories - (breakfast_cal + lunch_cal)
    # else:
    #     meal_index = len(df)
    #     #breakfast_id = list(df.loc[df['username'] == username]['Breakfast'])[-1] + 1
    #     breakfast_index = meal_index % len(breakfast_data)
    #     while check_hated_ingredient(breakfast_data['ingredients'][breakfast_index], hated_list):
    #         breakfast_index += 1
    #     breakfast_id = breakfast_data['id'][breakfast_index]
    #     breakfast_cal = breakfast_data['calories'][breakfast_index]
    #     #lunch_id = list(df.loc[df['username'] == username]['Lunch'])[-1] + 1
    #     lunch_index = meal_index % len(lunch_data)
    #     while check_hated_ingredient(breakfast_data['ingredients'][lunch_index], hated_list):
    #         lunch_index += 1
    #     lunch_id = lunch_data['id'][lunch_index]
    #     lunch_cal = lunch_data['calories'][lunch_index]
    #     remaining_cal = calories - (breakfast_cal + lunch_cal)

    #next_meal_index = len(df)
    # breakfast_id = list(df.loc[df['username'] == username]['Breakfast'])[-1] + 1
    #breakfast_index = next_meal_index % len(breakfast_data)
    breakfast_index = get_next_meal_index(df['Breakfast'], breakfast_data)
    while check_hated_ingredient(breakfast_data['ingredients'][breakfast_index], hated_list):
        breakfast_index = (breakfast_index + 1) % len(breakfast_data)
    breakfast_id = breakfast_data['id'][breakfast_index]
    breakfast_cal = breakfast_data['calories'][breakfast_index]
    # lunch_id = list(df.loc[df['username'] == username]['Lunch'])[-1] + 1
    lunch_index = get_next_meal_index(df['Lunch'], lunch_data)
    while check_hated_ingredient(lunch_data['ingredients'][lunch_index], hated_list):
        lunch_index = (lunch_index + 1) % len(lunch_data)
    lunch_id = lunch_data['id'][lunch_index]
    lunch_cal = lunch_data['calories'][lunch_index]
    remaining_cal = calories - (breakfast_cal + lunch_cal)
    dinner_index = get_next_meal_index(df['Dinner'], dinner_data)
    #dinner_id = dinner_data['id'][0]
    #for index in range(dinner_index, len(dinner_data)):
    while True:
        row = dinner_data.ix[dinner_index]
        if row['calories'] <= remaining_cal  and not check_hated_ingredient(dinner_data['ingredients'][dinner_index], hated_list):
            dinner_id = row['id']
            #dinner_cal = row['calories']
            break
        dinner_index = (dinner_index + 1) % len(dinner_data)
    #replace the following line by a database function
    #df.loc[counter] = [username, breakfast_id, lunch_id, dinner_id]
    #db.add_day_meals(username, breakfast_id, lunch_id, dinner_id) #this will be returned and the storing will occure outside the function
    return (breakfast_id, lunch_id, dinner_id)

def recommend_meals(username):
    user_object = db.get_user_by_username(username)
    answers_map = user_object.answers
    height = float(answers_map['height'][0])
    weight = float(answers_map['weight'][0])
    age = float(answers_map['age'][0])
    activity_level_string = str(answers_map['activity'][0])
    activity_level_float = float(weight_dictionary[activity_level_string])
    gender = answers_map['gender'][0]
    if gender == 'Male':
        daily_calories = daily_calorie_male(height, weight, age, activity_level_float)
    elif gender == 'Female':
        daily_calories = daily_calorie_female(height, weight, age, activity_level_float)
    (breakfast_id, lunch_id, dinner_id) = generate_meal_plan(user_object, daily_calories)
    return (breakfast_id, lunch_id, dinner_id)

def initialize_recommender():
    global breakfast_data
    global lunch_data
    global dinner_data
    global weight_dictionary
    breakfast_data = get_meal_data('Breakfast')
    lunch_data = get_meal_data('Lunch')
    dinner_data = get_meal_data('Dinner')
    weight_dictionary = get_weight_dictionary()


#initialization of the module takes place here
initialize_recommender()
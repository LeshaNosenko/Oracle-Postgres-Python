from jira import JIRA
import telebot
from telebot import types

bot = telebot.TeleBot('12345')

project_name = 'GISEHD'

def get_issue(jira, release):
    jql_query = f'project = {project_name} AND fixVersion = {release} AND "Оценка Проекта" is not EMPTY'
    issues = jira.search_issues(jql_query, maxResults=1)
    return issues[0] if issues else None

options = {
    'server': 'https://task.jira.ru'
}

jira = JIRA(options, basic_auth=('nosenkoan', 'testpswrd'))

fib = ['0', '1', '2', '3', '5', '8', '13', '21', '34', '55'] # Только 10 кнопок в опросе может быть(((

def podchet(release):
    jql_query = f'project = {project_name} AND fixVersion = {release} AND "Оценка Проекта" is not EMPTY'

    issues = jira.search_issues(jql_query)

    total = 0

    for jira.field in jira.fields():
        if jira.field['name'] == 'Оценка Проекта':
           v_ocenka_field = jira.field ['id']

    if not v_ocenka_field:
       return

    for issue in issues:
        cnt_ehd = getattr( issue.fields, v_ocenka_field, None )
        if cnt_ehd is not None:
           total += cnt_ehd

    return total

def getReleases(jira):
    dictVersions = jira.project_versions(project_name)
    release_names = [version.name for version in dictVersions]
    return release_names

def is_administrator(chat_id, user_id):
    chat_member = bot.get_chat_member( chat_id, user_id )
    if chat_member and (chat_member.status == 'creator' or chat_member.status == 'administrator'):
        return True
    return False

user_podschets = {}

@bot.message_handler(commands=['start'])
def start(message):
    chat_id = message.chat.id
    if is_administrator(chat_id, message.from_user.id):
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        btn1 = types.KeyboardButton('Выгрузить оценки Проекта')
        markup.add(btn1)
        btn2 = types.KeyboardButton('Создать новый опрос на оценку')
        markup.add(btn2)
        btn3 = types.KeyboardButton('Удалить бота из группы')
        markup.add(btn3)
        bot.send_message(chat_id,
                         'Привет! Я твой бот-помощник! \n\n'
                         'Если хочешь, чтобы я предоставил оценки Проекта по релизам Проекта, нажми на кнопку "Выгрузить оценки Проекта \n\n'
                         'Для создания опроса на оценку нажми на кнопку "Создать новый опрос на оценку"',
                         reply_markup=markup)
    else:
        bot.send_message(chat_id, "Извините, вы не являетесь администратором и не имеете доступа к этим функциям.")


@bot.message_handler(content_types=['text'])
def get_text_messages(message):
    chat_id = message.chat.id
    if is_administrator(chat_id, message.from_user.id):
        if message.text == 'Выгрузить оценки ':
            getReleases(jira)
            releases = getReleases(jira)
            summa_ehd = ""
            for release in releases:
                total = podchet(release)
                summa_ehd += f'Сумма оценки по релизу: {release} = {round(total)}\n'
            bot.send_message(chat_id, summa_ehd)
        elif message.text == 'Создать новый опрос на оценку':
            user_podschets.clear()
            question = "Опрос по оценке по задаче по релизу успешно создан. Прошу её оценить!"
            options = [f"{num}" for num in fib]
            bot.send_poll(chat_id, question, options=options, is_anonymous=False)
            markup = types.InlineKeyboardMarkup()
            calc_button = types.InlineKeyboardButton(text="Рассчитать", callback_data="calculate", resize_keyboard=True)
            markup.add(calc_button)
            bot.send_message(chat_id, "Для расчета средней оценки нажмите на кнопку рассчитать!", reply_markup=markup)
        elif message.text == 'Рассчитать':
            calc_ocenki(chat_id)
        elif message.text == 'Удалить бота из группы':
            bot.leave_chat(chat_id)
    else:
        bot.send_message(chat_id, "Извините, вы не являетесь администратором и не имеете доступа к этим функциям.")

@bot.poll_answer_handler()
def handle_poll_answer(poll_answer: types.PollAnswer):
    user_id = poll_answer.user.id
    option_id = int(poll_answer.option_ids[0])
    selected_number = int(fib[option_id]) # Тут как-то тупо возвращает хендлер, возвращает индекс выбранного элемента в опросе, а не само значение, поэтому значение я вытягиваю по индексу...
    user_podschets.setdefault(user_id, []).append(selected_number)

def calc_ocenki(chat_id):
    total_estimate = 0
    total_users = len(user_podschets)
    for podschets in user_podschets.values():
        total_estimate += sum(podschets)
    if total_users != 0:
        average_estimate = total_estimate / total_users
        bot.send_message(chat_id, f"Средняя оценка по задаче: {round(average_estimate)}")
    else:
        bot.send_message(chat_id, "Нет пользовательских оценок.")

@bot.callback_query_handler(func=lambda call: call.data == "calculate")
def handle_calculation_query(call):
    chat_id = call.message.chat.id
    calc_ocenki(chat_id)

bot.add_poll_answer_handler(handle_poll_answer)
bot.polling(none_stop=True, interval=0)
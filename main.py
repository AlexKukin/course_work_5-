from src.head_hunter_api import HeadHunterAPI
from config import config
from src.db_manager import DbManager
import psycopg2


EMPLOYERS_DICT = {"5856776": "Зерокодер",
                  "2642624": "ТК Прогресс",
                  "1235466": "ABS GROUP",
                  "5686111": "Olima",
                  "9301808": "Сбер Бизнес Софт",
                  "783222": "Leads",
                  "1040211": "Группа компаний eLama",
                  "1622442": "Максимал",
                  "4767781": "AlfaBit",
                  "3365917": "Cleverest Technologies"}


def main():
    """Метод загрузки вакансий с сайта HH от заданных компаний и вывода информации по ним.
     Точка входа в программу."""

    print('Загрузка вакансий следующих компаний:\n' + ('"\n"'.join(EMPLOYERS_DICT.values())) + "\n")

    hh_api = HeadHunterAPI()
    vacancies = hh_api.load_vacancies(params={'employer_id': EMPLOYERS_DICT.keys()})

    params = config()
    try:

        with DbManager(db_name='my_new_db', params=params) as db_manager:

            db_manager.drop_db()
            db_manager.create_db()

            db_manager.creat_employers_table()
            print(f"Создана таблица employers")

            db_manager.create_vacancies_table()
            print(f"Создана таблица vacancies")

            # заполнение таблиц БД
            db_manager.fill_database(vacancies)
            print(f"Таблицы заполнены данным")

            db_manager.print_info()

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == '__main__':
    main()

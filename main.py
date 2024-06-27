from src.head_hunter_api import HeadHunterAPI
from config import config
from src.db_manager import DbManager


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
    db_manager = DbManager(db_name='my_new_db', params=params)

    # пересоздание и заполнение БД
    db_manager.create_and_fill_db(vacancies)

    # Вывод различных выборок по вакансиям и компаниям
    db_manager.print_info()


if __name__ == '__main__':
    main()

from src.file_worker import JsonWorker
from src.head_hunter_api import HeadHunterAPI
import os.path
from config import config, DATA_DIR_PATH
from src.db_manager import DbManager


# По данному ключевому слову будет выполнять request c hh
JSON_FILE_PATH = os.path.join(DATA_DIR_PATH, "vacancies.json")

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

    print('Загрузка возможна для вакансий следующих компаний:\n' + ('"\n"'.join(EMPLOYERS_DICT.values())) + "\n")

    json_worker = JsonWorker(JSON_FILE_PATH)
    if not os.path.exists(JSON_FILE_PATH) \
            or input('Json файл вакансий существует. Пересоздать его ? (да / нет)\n').lower().strip() == "да":
        # Создание экземпляра класса для работы с API сайтов с вакансиями
        hh_api = HeadHunterAPI(json_worker)
        vacancies = hh_api.load_vacancies(params={'employer_id': EMPLOYERS_DICT.keys()})  # 'text':'Python'}) # ,
        # Сохраняет json файл c вакансиями на диск. Перезаписывает, если файл существует
        hh_api.file_worker.save_vacancies_to_disk(vacancies)
    else:
        vacancies = json_worker.read_vacancies_from_disk()

    params = config()
    db_manager = DbManager(db_name='my_new_db', params=params)

    # создание и заполнение БД
    db_manager.create_and_fill_db(vacancies)

    # Вывод информации о вакансиях и компаниях
    db_manager.print_info()


if __name__ == '__main__':
    main()

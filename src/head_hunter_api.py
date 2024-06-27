import requests
from abc import ABC, abstractmethod
from src.vacancy import Vacancy


class HeadHunterAPI:
    """
    Класс для работы с API HeadHunter
    """

    def __init__(self):
        self.url = 'https://api.hh.ru/vacancies'
        self.headers = {'User-Agent': 'HH-User-Agent'}
        self.def_params = {'page': 0, 'per_page': 100}
        super().__init__()

    def load_vacancies(self, params):
        """ Загрузка вакансий из удаленного ресурса HH.ru"""
        params = {**self.def_params, **params}
        hh_vacancies = []
        page_max_count = 10
        while params.get('page') != page_max_count:
            response = requests.get(self.url, headers=self.headers, params=params)
            params['page'] += 1
            print(f"Чтение страницы hh №{params['page'] } из {page_max_count}")
            hh_vacancies.extend(response.json()['items'])
        params['page'] = 0
        print() # hh_vacancies)
        return Vacancy.cast_to_object_list(hh_vacancies)

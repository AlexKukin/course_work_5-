from src.vacancy import Vacancy
from typing import List


class Utils:
    """Класс со вспомогательными методами"""

    @staticmethod
    def filter_vacancies(vacancies: List[Vacancy], filter_words):
        """Фильтрация вакансий по наличию в содержимом ряда атрибутов хотя бы одного из ключевых слов filter_words"""
        if len(filter_words) == 1 and filter_words[0].strip() == '':
            return vacancies

        return [vacancy for vacancy in vacancies
                if all(filter_word in vacancy.name.lower()
                       or filter_word in vacancy.responsibility.lower()
                       or filter_word in vacancy.requirement.lower()
                       for filter_word in filter_words)]

    @staticmethod
    def get_vacancies_by_salary(vacancies: List[Vacancy], salary_range: range):
        """Получение вакансии с ЗП в указанном диапазоне salary_range. Не учитывается валюта оплаты"""
        filter_vacancies = [vacancy for vacancy in vacancies
                            if Utils.non_empty_intersection(vacancy.salary.get_range(), salary_range)]
        return filter_vacancies

    @staticmethod
    def non_empty_intersection(a, b):
        """Поиск есть ли пересечение двух множеств"""
        smaller, bigger = a, b
        if len(a) < len(b):
            smaller, bigger = bigger, smaller
        for e in smaller:
            if e in bigger:
                return True
        return False

    @staticmethod
    def sort_vacancies_by_salary(vacancies: List[Vacancy]):
        """Сортировка вакансий по возрастанию ЗП"""
        vacancies = sorted(vacancies,
                           key=lambda x: x.salary.get_range()[-1],
                           reverse=True)
        return vacancies

    @staticmethod
    def get_top_vacancies(vacancies: List[Vacancy], top_n):
        """ Возвращает первые top_n шт. вакансий"""
        return vacancies[:top_n]

    @staticmethod
    def print_vacancies(vacancies: List[Vacancy]):
        """Вывод информации в консоль о вакансиях"""

        if not vacancies:
            print("\nПо вашему запросу вакансии не найдены...")
            return

        print("\nРезультаты запроса:\n")
        for idx, _vacancy in enumerate(vacancies):
            print(f"{idx + 1}. {_vacancy}")

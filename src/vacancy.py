from typing import List
from src.salary_range import SalaryRange
from src.employer import Employer


class Vacancy:

    def __init__(self, _id: int, name: str, url: str,  salary: SalaryRange, employer: Employer):
        self.id = _id
        self.name = name
        self.url = url
        self.salary = salary
        self.employer = employer

    def __str__(self):
        return (f"Название: {self.name}\n"
                f"Зарплата: {self.salary}\n"
                f"Url: {self.url}\n")

    def __eq__(self, other):
        return isinstance(other, Vacancy) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @staticmethod
    def cast_to_object_list(dct_vacancies: List[dict]):
        """Преобразует список hh_vacancies в список объектов Vacancy"""

        def hh_to_object(dct_vacancy: dict):
            """Преобразует  hh_vacancy в объект Vacancy"""
            _id = int(dct_vacancy.get('id'))
            name = dct_vacancy.get('name', '')
            url = dct_vacancy.get('url', '')
            # responsibility = Vacancy.get_without_none(dct_vacancy.get('snippet', {}), 'responsibility')

            salary_dict = dct_vacancy.get('salary')
            salary = SalaryRange() if not salary_dict else SalaryRange(salary_dict['from'], salary_dict['to'])

            # employer_dict = dct_vacancy.get('employer')
            employer_id = int(Vacancy.get_without_none(dct_vacancy.get('employer', {}), 'id', replacement=999999))
            employer_name = Vacancy.get_without_none(dct_vacancy.get('employer', {}), 'name')

            return Vacancy(_id, name, url, salary, Employer(employer_id, employer_name))

        vacancies = list(map(hh_to_object, dct_vacancies))
        return vacancies

    @staticmethod
    def to_object(dct_vacancy: dict):
        """Преобразует dict в объект Vacancy"""
        _id = dct_vacancy.get('id')
        name = dct_vacancy.get('name', '')
        url = dct_vacancy.get('url', '')
        # responsibility = Vacancy.get_without_none(dct_vacancy, 'responsibility')
        salary_from = dct_vacancy.get('salary_from')
        salary_to = dct_vacancy.get('salary_to')

        employer_id = int(dct_vacancy.get('employer_id'))
        employer_name = dct_vacancy.get('employer_name')

        return Vacancy(_id, name, url, SalaryRange(salary_from, salary_to), Employer(employer_id, employer_name))

    @staticmethod
    def get_without_none(dct, key, replacement=''):
        """При чтении значений словаря dct заменят любые случаи возвращения None на значение replacement"""
        value = dct.get(key, replacement)
        if not value:
            value = replacement
        return value

    def to_dict(self):
        """Преобразует текущий объект Vacancy в  dict"""
        return {
            'id': self.id,
            'name': self.name,
            'url': self.url,
            'salary_from': self.salary.from_salary,
            'salary_to': self.salary.to_salary,
            'employer_id': self.employer.id,
            'employer_name': self.employer.name,
        }

    def to_sql_dict(self):
        """Преобразует текущий объект Vacancy в  dict для помещения в таблицу SQL"""
        return {
            'vacancy_id': self.id,
            'employer_id': self.employer.id,
            'vacancy_name': self.name,
            'url': self.url,
            'salary_from': self.salary.from_salary,
            'salary_to': self.salary.to_salary,
        }

    # salary getter and setter
    @property
    def salary(self):
        return self.__salary

    @salary.setter
    def salary(self, value: SalaryRange):
        if not isinstance(value, SalaryRange):
            raise TypeError(f'Значение value должно иметь тип {SalaryRange.__name__}')
        self.__salary = value



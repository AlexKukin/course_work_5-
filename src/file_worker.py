from abc import ABC, abstractmethod
from src.vacancy import Vacancy
from typing import List
import os
import json


class FileWorker(ABC):

    def __init__(self, file_path: str):
        self.file_path = file_path

    @abstractmethod
    def save_vacancies_to_disk(self, vacancies: List[Vacancy]):
        """Сохранение полученных вакансии в файл на диск"""
        pass

    @abstractmethod
    def read_vacancies_from_disk(self):
        """Чтение вакансий из файла на диске"""
        pass


class JsonWorker(FileWorker):

    def __init__(self, file_path: str):
        super().__init__(file_path)

    def save_vacancies_to_disk(self, vacancies: List[Vacancy]):
        """Сохранение полученных вакансии в Json файл на диск"""
        with open(self.file_path, 'w', encoding="utf-8") as file:
            json.dump(vacancies,
                      file,
                      default=lambda o: o.to_dict(),
                      indent=4,
                      ensure_ascii=False)

    def read_vacancies_from_disk(self) -> List[Vacancy]:
        """Чтение вакансий из Json файла на диске"""
        if not os.path.isfile(self.file_path):
            return []

        with open(self.file_path, 'r', encoding="utf-8") as file:
            data = json.load(file, object_hook=Vacancy.to_object)
        return data

import psycopg2
from psycopg2.extras import execute_batch
from src.vacancy import Vacancy


class DbManager:
    """Осуществляет выборку данных из sql БД по фильтрам"""

    def __init__(self, db_name, params):
        self.db_name = db_name
        self.params = params

    def create_and_fill_db(self, vacancies):
        # создание БД
        DbManager.create_db(self.params, self.db_name)

        self.params.update({'dbname': self.db_name})
        conn = None
        try:
            with psycopg2.connect(**self.params) as conn:
                with conn.cursor() as cur:
                    # создание схем таблиц БД
                    DbManager.creat_employers_table(cur)
                    print(f"Создана таблица employers")

                    DbManager.create_vacancies_table(cur)
                    print(f"Создана таблица vacancies")

                    # заполнение таблиц БД
                    DbManager.fill_database(cur, vacancies)
                    print(f"Таблицы заполнены данным из json")

                    # Добавление связей по внешнему ключу
                    DbManager.add_foreign_keys(cur)
                    print(f"Добавлены внешние ключи")

        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def create_db(params, db_name) -> None:
        """Создает новую базу данных c именем db_name."""
        try:
            conn = psycopg2.connect(**params)
            cursor = conn.cursor()

            conn.autocommit = True
            add_db_query = f"CREATE DATABASE {db_name}"

            # выполняем код sql
            cursor.execute(add_db_query)
            print(f"База данных {db_name} успешно создана")

            cursor.close()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def create_vacancies_table(cur) -> None:
        """Создает таблицу vacancies."""
        cur.execute("""
        CREATE TABLE vacancies (
            vacancy_id INT PRIMARY KEY,
            employer_id INT,
            vacancy_name VARCHAR(255) NOT NULL,
            salary_from INT,
            salary_to INT,
            url VARCHAR(255) NOT NULL
        )
        """)

    @staticmethod
    def creat_employers_table(cur) -> None:
        """Создает таблицу employers."""
        cur.execute("""
        CREATE TABLE employers (
            employer_id INT PRIMARY KEY,
            employer_name VARCHAR(255) NOT NULL
        )
        """)

    @staticmethod
    def fill_database(cur, vacancies: list[Vacancy]) -> None:
        """Заполняет таблицы БД и связываем внешние ключи"""

        vacancies_dicts = [vacancy.to_sql_dict() for vacancy in vacancies]
        employers_dicts = []
        seen_employer_ids = []
        for vacancy in vacancies:
            if vacancy.employer.id not in seen_employer_ids:
                employers_dicts.append(vacancy.employer.to_sql_dict())
                seen_employer_ids.append(vacancy.employer.id)

        # Заполняем sql таблицы из json
        DbManager.insert_vacancies_data(cur, vacancies_dicts)
        DbManager.insert_employers_data(cur, employers_dicts)

    @staticmethod
    def insert_vacancies_data(cur, vacancies: list[dict]) -> None:
        """Добавляет данные из vacancies в таблицу vacancies."""
        add_vacancies_query = (f"""INSERT INTO vacancies
                      (vacancy_id, employer_id, vacancy_name, salary_from, salary_to, url) 
                      VALUES (%(vacancy_id)s, %(employer_id)s, %(vacancy_name)s, %(salary_from)s, %(salary_to)s, %(url)s)
                      """)

        execute_batch(cur, add_vacancies_query, vacancies)

    @staticmethod
    def insert_employers_data(cur, employers: list[dict]) -> None:
        """Добавляет данные из employers в таблицу employers."""
        add_employers_query = (f"""INSERT INTO employers
                         (employer_id, employer_name) 
                         VALUES (%(employer_id)s, %(employer_name)s)""")
        execute_batch(cur, add_employers_query, employers)

    @staticmethod
    def add_foreign_keys(cur) -> None:
        """Добавляет foreign key с на employer_id в таблицу vacancies."""
        add_foreign_keys_query = (f""" 
                        ALTER TABLE vacancies 
                        ADD CONSTRAINT fk_employer
                        FOREIGN KEY(employer_id) 
                        REFERENCES employers(employer_id) """)

        cur.execute(add_foreign_keys_query)

    def get_employers_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""
        with psycopg2.connect(**self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT employer_name, COUNT(*)
                FROM employers
                LEFT JOIN vacancies USING (employer_id)
                GROUP BY employer_name
                """)
                rows = cur.fetchall()
        conn.close()
        return rows

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию"""
        with psycopg2.connect(**self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT employer_name, vacancy_name, salary_from, salary_to, url 
                FROM vacancies 
                JOIN employers USING (employer_id)
                ORDER BY employer_name, salary_from, salary_to
                """)
                rows = cur.fetchall()
        conn.close()
        return rows

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям"""
        with psycopg2.connect(**self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT AVG(salary_from) as salary_from_avg
                FROM vacancies
                WHERE salary_from IS NOT NULL
                """)
                avg_salary = cur.fetchone()
        conn.close()
        return avg_salary

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        with psycopg2.connect(**self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT vacancy_name, salary_from, salary_to
                FROM vacancies 
                WHERE salary_from > (SELECT AVG(salary_from)
                                    FROM vacancies
                                    WHERE salary_from IS NOT NULL) 
                or salary_to > (SELECT AVG(salary_from)
                                FROM vacancies
                                WHERE salary_from IS NOT NULL)
                ORDER BY salary_from, salary_to
                """)
                rows = cur.fetchall()
        conn.close()
        return rows

    def get_vacancies_with_keyword(self, keyword: str):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python"""
        with psycopg2.connect(**self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT vacancy_name
                FROM vacancies 
                WHERE LOWER(vacancy_name) LIKE '%{keyword.lower()}%'
                ORDER BY salary_from, salary_to
                """)
                rows = cur.fetchall()
        conn.close()
        return rows

    def print_info(self):
        """Выводит результат различных запросов к БД к таблицам с компаниями и вакансиями"""

        print(f"\n1. Cписок всех компаний и количество вакансий у каждой компании:")
        [print(f"{item[0]}: {item[1]}шт.") for item in self.get_employers_and_vacancies_count()]

        print(f"\n2. Cписок всех вакансий с указанием названия компании,названия вакансии и зарплаты "
              f"и ссылки на вакансию:")
        [print(item) for item in self.get_all_vacancies()]

        salary_avg = round(self.get_avg_salary()[0], 2)
        print(f"\n3. Cредняя зарплата по вакансиям:\n{salary_avg}")

        print(f"\n4. Список всех вакансий, у которых зарплата выше средней ({salary_avg}) по всем вакансиям:")
        [print(item) for item in self.get_vacancies_with_higher_salary()]

        print(f"\n5. Список всех вакансий, в названии которых содержатся переданные в метод слова, например python:")
        [print(f"{item[0]}") for item in self.get_vacancies_with_keyword(keyword='Python')]


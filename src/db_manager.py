import psycopg2
from psycopg2.extras import execute_batch
from src.vacancy import Vacancy


class DbManager:
    """Управляет подключением к БД. Осуществляет выборку данных из sql БД по фильтрам"""

    query_cases_dict = {"1": "Список всех компаний и количество вакансий у каждой компании",
                        "2": "Список всех вакансий с указанием названия компании,названия вакансии"
                             " и зарплаты и ссылки на вакансию",
                        "3": "Cредняя зарплата по вакансиям",
                        "4": "Список всех вакансий, у которых зарплата выше средней по всем вакансиям",
                        "5": "Список всех вакансий, в названии которых содержатся переданные в метод слова,"
                             " например python",
                        "0": "Выход"}

    def __init__(self, db_name, params):
        self.db_name = db_name
        self.params = params
        self._connection = None

    def __enter__(self):
        self.create_connection()
        return self

    def __exit__(self, *exc):
        self.close_connection()

    def create_connection(self) -> None:
        if not self._connection:
            self._connection = psycopg2.connect(**self.params)

    def close_connection(self) -> None:
        if self._connection:
            self._connection.close()

    def create_db(self) -> None:
        self.execute_sql_query(f"CREATE DATABASE {self.db_name}")

    def drop_db(self) -> None:
        self.execute_sql_query(f"DROP DATABASE {self.db_name}")

    def execute_sql_query(self, query: str, is_autocommit: bool = True) -> None:
        """Выполняет запрос query к БД"""
        try:
            with self._connection.cursor() as cur:
                self._connection.autocommit = is_autocommit
                cur.execute(query)
                print(f"Команда {query} успешно выполнена.")

        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self._connection:
                self._connection.autocommit = False

    def create_vacancies_table(self) -> None:
        """Создает таблицу vacancies."""
        with self._connection.cursor() as cur:
            cur.execute("""
            CREATE TABLE vacancies (
                vacancy_id INT PRIMARY KEY,
                employer_id INT,
                vacancy_name VARCHAR(255) NOT NULL,
                salary_from INT,
                salary_to INT,
                url VARCHAR(255) NOT NULL,
                CONSTRAINT fk_employer
                            FOREIGN KEY(employer_id) 
                            REFERENCES employers(employer_id)
        )
        """)

    def creat_employers_table(self) -> None:
        """Создает таблицу employers."""
        with self._connection.cursor() as cur:
            cur.execute("""
            CREATE TABLE employers (
                employer_id INT PRIMARY KEY,
                employer_name VARCHAR(255) NOT NULL
            )
            """)

    def fill_database(self, vacancies: list[Vacancy]) -> None:
        """Заполняет таблицы БД и связываем внешние ключи"""
        vacancies_dicts = [vacancy.to_sql_dict() for vacancy in vacancies]
        employers_dicts = []
        seen_employer_ids = []
        for vacancy in vacancies:
            if vacancy.employer.id not in seen_employer_ids:
                employers_dicts.append(vacancy.employer.to_sql_dict())
                seen_employer_ids.append(vacancy.employer.id)

        # Заполняем sql таблицы из json
        self.insert_employers_data(employers_dicts)
        self.insert_vacancies_data(vacancies_dicts)

    def insert_vacancies_data(self, vacancies: list[dict]) -> None:
        """Добавляет данные из vacancies в таблицу vacancies."""
        with self._connection.cursor() as cur:
            add_vacancies_query = (f"""INSERT INTO vacancies
                          (vacancy_id, employer_id, vacancy_name, salary_from, salary_to, url) 
                          VALUES (%(vacancy_id)s, %(employer_id)s, %(vacancy_name)s, %(salary_from)s, %(salary_to)s, %(url)s)
                          """)

            execute_batch(cur, add_vacancies_query, vacancies)

    def insert_employers_data(self, employers: list[dict]) -> None:
        """Добавляет данные из employers в таблицу employers."""
        with self._connection.cursor() as cur:
            add_employers_query = (f"""INSERT INTO employers
                             (employer_id, employer_name) 
                             VALUES (%(employer_id)s, %(employer_name)s)""")
            execute_batch(cur, add_employers_query, employers)

    def get_employers_and_vacancies_count(self) -> list:
        """Получает список всех компаний и количество вакансий у каждой компании"""
        with self._connection.cursor() as cur:
            cur.execute(f"""
                            SELECT employer_name, COUNT(*)
                            FROM employers
                            LEFT JOIN vacancies USING (employer_id)
                            GROUP BY employer_name
                            """)
            return cur.fetchall()

    def get_all_vacancies(self) -> list:
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию"""
        with self._connection.cursor() as cur:
            cur.execute(f"""
                           SELECT employer_name, vacancy_name, salary_from, salary_to, url 
                           FROM vacancies 
                           JOIN employers USING (employer_id)
                           ORDER BY employer_name, salary_from, salary_to
                           """)
            return cur.fetchall()

    def get_avg_salary(self) -> dict:
        """Получает среднюю зарплату по вакансиям"""
        with self._connection.cursor() as cur:
            cur.execute(f"""
                         SELECT AVG(salary_from) as salary_from_avg
                         FROM vacancies
                         WHERE salary_from IS NOT NULL
                         """)
            avg_salary = cur.fetchone()
            return avg_salary

    def get_vacancies_with_higher_salary(self) -> list:
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        with self._connection.cursor() as cur:
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
            return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword: str) -> list:
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python"""
        with self._connection.cursor() as cur:
            cur.execute(f"""
                            SELECT vacancy_name
                            FROM vacancies 
                            WHERE LOWER(vacancy_name) LIKE '%{keyword.lower()}%'
                            ORDER BY salary_from, salary_to
                            """)
            return cur.fetchall()

    def print_info(self) -> None:
        """Выводит результат различных запросов к БД к таблицам с компаниями и вакансиями, согласно выбранному
        номеру case_num"""
        while True:
            print("\nВарианты вывода:")
            [print(f"{k}: {v};") for k, v in DbManager.query_cases_dict.items()]
            case_num = input("Введите номер варианта: ").strip()

            if case_num == "1":
                print(f"\n1. Список всех компаний и количество вакансий у каждой компании:")
                [print(f"{item[0]}: {item[1]}шт.") for item in self.get_employers_and_vacancies_count()]

            elif case_num == "2":
                print(f"\n2. Список всех вакансий с указанием названия компании,названия вакансии и зарплаты "
                      f"и ссылки на вакансию:")
                [print(item) for item in self.get_all_vacancies()]

            elif case_num == "3":
                salary_avg = round(self.get_avg_salary()[0], 2)
                print(f"\n3. Cредняя зарплата по вакансиям:\n{salary_avg}")

            elif case_num == "4":
                salary_avg = round(self.get_avg_salary()[0], 2)
                print(f"\n4. Список всех вакансий, у которых зарплата выше средней ({salary_avg}) по всем вакансиям:")
                [print(item) for item in self.get_vacancies_with_higher_salary()]

            elif case_num == "5":
                print(f"\n5. Список всех вакансий, в названии которых содержатся переданные в метод слова, "
                      f"например python:")
                [print(f"{item[0]}") for item in self.get_vacancies_with_keyword(keyword='Python')]

            elif case_num == "0":
                print(f"\n0. Выход из выбора вариантов.")
                break
            else:
                print(f"\nНеверно задан номер, повторите ввод.")

            input('\nНажмите Enter, чтобы продолжить...')

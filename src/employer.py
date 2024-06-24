class Employer:

    def __init__(self, _id: int, _name: str):
        self.id = _id
        self.name = _name

    def __str__(self):
        return f"Компания: {self.employer_name}"

    def to_sql_dict(self):
        """Преобразует текущий объект Employer в  dict для помещения в таблицу SQL"""
        return {
              'employer_id': self.id,
              'employer_name': self.name
              }


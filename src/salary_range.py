class SalaryRange:
    def __init__(self, from_salary: int = None, to_salary: int = None):
        self.from_salary = from_salary
        self.to_salary = to_salary

    def __str__(self):
        if self.from_salary and self.to_salary:
            return f"{self.from_salary} - {self.to_salary}"
        elif not self.from_salary and not self.to_salary:
            return 'Зарплата не указана'
        else:
            concreate_salary = self.to_salary if not self.from_salary else self.from_salary
            return f"{concreate_salary}"

    def get_range(self) -> range:
        if self.from_salary and self.to_salary:
            return range(self.from_salary, self.to_salary + 1)
        elif not self.from_salary and not self.to_salary:
            return range(0, 1)
        else:
            concreate_salary = self.to_salary if not self.from_salary else self.from_salary
            return range(concreate_salary, concreate_salary + 1)



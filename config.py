import os.path
from configparser import ConfigParser

ROOT_PATH = os.path.dirname(__file__)
DATA_DIR_PATH = os.path.join(ROOT_PATH, 'data')
DB_CONN_FILE_PATH = os.path.join(DATA_DIR_PATH, "database.ini")
CONFIG_SQL_FILE_PATH = os.path.join(DATA_DIR_PATH, DB_CONN_FILE_PATH)


# def config():
#     parser = ConfigParser()
#     file_name = os.path.join(DATA_DIR_PATH, DB_CONN_FILE_PATH)
#     parser.read(file_name)
#     confdict = {section: dict(parser.items(section)) for section in parser.sections()}
#     return confdict


def config(filename=CONFIG_SQL_FILE_PATH, section="postgresql"):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} is not found in the {1} file.'.format(section, filename))
    return db
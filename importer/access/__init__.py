from libs import database


# --[ CHECKS ]----------------------------------------------------------------------------------------------------------
def postal_code_exist(postal_code, house_number):
    if not postal_code:
        return False

    if not house_number:
        return False

    sql = 'SELECT TRUE AS exist FROM postal_codes WHERE code=? AND house_number_min>=? AND house_number_max<=?'
    if database.mariadb_execute_sql(sql, (postal_code, house_number, house_number), fetch_one=True):
        return True
    else:
        return False


# --[ CLEANUP ]---------------------------------------------------------------------------------------------------------
def cleanup_postal_code(postal_code):
    return str(postal_code).replace(" ", "")


def cleanup_house_number(house_number):
    if not house_number:
        return None

    value = str(house_number).replace(" ", "")
    if value.isnumeric():
        return value

    count = len(value)
    while count > 0 and not value[:count].isnumeric():
        count -= 1

    return value[:count]


def cleanup_huisnummertoevoeging(house_number, house_number_addition):
    if not house_number_addition:
        return None

    return house_number_addition.replace(house_number, "", 1)

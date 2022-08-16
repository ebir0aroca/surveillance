def remove_chars(s):
    s2 = re.sub(r'[^0-9.,]+', '', s)
    return s2.replace(",", ".")
